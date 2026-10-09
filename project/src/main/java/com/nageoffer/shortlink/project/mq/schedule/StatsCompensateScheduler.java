/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.shortlink.project.mq.schedule;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nageoffer.shortlink.project.dao.entity.LinkStatsCompensateDO;
import com.nageoffer.shortlink.project.dao.mapper.LinkStatsCompensateMapper;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.LOCK_STATS_COMPENSATE_SCHEDULER_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_TOPIC_KEY;
import static org.apache.rocketmq.spring.support.RocketMQHeaders.KEYS;

/**
 * 短链接统计 MQ 失败补偿定时任务
 * 每 60 秒扫描一次 t_link_stats_compensate 中 status=0 且创建时间超过 60s 的记录，
 * 批量 syncSend 重投 RocketMQ；成功改状态，失败 retry_cnt+1，超过 10 次标记永久失败。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatsCompensateScheduler {

    private static final int BATCH_SIZE = 200;
    private static final int SINGLE_ROUND_LIMIT = 2000;
    private static final int MAX_RETRY_CNT = 10;
    private static final long RESEND_TIMEOUT_MS = 500L;
    /**
     * 单轮巡检执行超时时间（秒）：超过则熔断中断，防止工作线程卡死导致锁被无限持有
     */
    private static final long TASK_TIMEOUT_SECONDS = 300L;

    private final LinkStatsCompensateMapper compensateMapper;
    private final RocketMQTemplate rocketMQTemplate;
    private final ObjectMapper objectMapper;
    private final RedissonClient redissonClient;

    /**
     * 独立工作线程：持锁的调度线程只负责提交任务和计时，干活交给它。
     * 工作线程即使卡死/死循环，调度线程也能到点走 finally 释放锁，锁不会被永久霸占
     */
    private final ExecutorService compensateExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "stats-compensate-worker");
        t.setDaemon(true);
        return t;
    });

    /**
     * 记录当前正在执行的工作任务：上一轮还没结束（卡住）时跳过本轮提交，避免任务在队列里堆积
     */
    private final AtomicReference<Future<?>> runningTask = new AtomicReference<>();

    /**
     * fixedDelay=60s：上一次执行跑完之后再等 60 秒开始下一轮，避免 Broker 挂时上一轮没跑完下一轮又堆积
     * 分布式锁：多实例部署时保证全集群同一时刻只有一个实例执行巡检，避免重复重投和 retry_cnt 竞态
     * 超时熔断：任务提交到独立工作线程执行，调度线程最多等 TASK_TIMEOUT_SECONDS，到点必然释放锁
     */
    @Scheduled(fixedDelay = 60_000L, initialDelay = 30_000L)
    public void compensateOnce() {
        RLock lock = redissonClient.getLock(LOCK_STATS_COMPENSATE_SCHEDULER_KEY);
        boolean locked;
        try {
            // waitTime=0：抢不到锁立即放弃（其他实例正在执行，本轮跳过即可，补偿晚 60 秒无影响）
            // 不指定 leaseTime：启用 Redisson watchdog（30s TTL 自动续期），任务跑多久锁就持有多久。
            // 死循环保护：干活在独立工作线程，调度线程最多等 TASK_TIMEOUT_SECONDS 就必然走到 unlock，
            // watchdog 随 unlock 停止 —— 即使工作线程卡死，锁也最多被持有约 5 分钟，不会永久霸占
            locked = lock.tryLock(0, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        if (!locked) {
            return;
        }
        try {
            Future<?> previous = runningTask.get();
            if (previous != null && !previous.isDone()) {
                // 上一轮工作线程还没跑完（可能卡在 Broker 超时重试上），本轮跳过，锁直接释放
                log.warn("上一轮补偿任务仍在执行，本轮跳过");
                return;
            }
            Future<?> future = compensateExecutor.submit(this::doCompensate);
            runningTask.set(future);
            try {
                // 调度线程在此阻塞等待，最多 TASK_TIMEOUT_SECONDS，超时即熔断
                future.get(TASK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                // 中断工作线程：卡在 sleep/可中断 IO 上会立即退出；卡在网络硬等待上退不出也无妨，
                // 锁马上释放，且 runningTask 守卫会阻止后续轮次继续堆积任务
                future.cancel(true);
                log.error("补偿任务执行超过 {}s，触发超时熔断，中断本轮", TASK_TIMEOUT_SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                log.error("补偿任务执行异常", e.getCause());
            }
        } finally {
            lock.unlock();
        }
    }

    private void doCompensate() {
        int totalHandled = 0;
        int successCnt = 0;
        int failCnt = 0;
        int permanentFailCnt = 0;

        while (totalHandled < SINGLE_ROUND_LIMIT) {
            // 一次处理 BATCH_SIZE 条记录
            var list = compensateMapper.selectPendingBatch(BATCH_SIZE);
            if (list == null || list.isEmpty()) {
                // 如果没有需要处理的任务就return
                break;
            }

            for (LinkStatsCompensateDO row : list) {
                totalHandled++;
                try {
                    ShortLinkStatsRecordDTO dto = objectMapper.readValue(row.getPayloadJson(), ShortLinkStatsRecordDTO.class);
                    Message<ShortLinkStatsRecordDTO> msg = MessageBuilder
                            .withPayload(dto)
                            .setHeader(KEYS, row.getMsgKeys())   // 复用原来的 KEYS UUID，消费者 Redis 幂等仍生效！
                            .build();
                    rocketMQTemplate.syncSend(SHORT_LINK_STATS_TOPIC_KEY, msg, RESEND_TIMEOUT_MS);
                    compensateMapper.updateSuccessById(row.getId());
                    successCnt++;
                } catch (Exception sendOrParseEx) {
                    Integer currentRetry = row.getRetryCnt() == null ? 0 : row.getRetryCnt();
                    if (currentRetry >= MAX_RETRY_CNT) {
                        // 重试次数大于最大的失败次数，标记为永久失败
                        compensateMapper.markPermanentFailById(row.getId());
                        permanentFailCnt++;
                        log.error("统计补偿永久失败（已重试{}次），人工介入 id={} msgKeys={} fullShortUrl={}",
                                currentRetry, row.getId(), row.getMsgKeys(), row.getFullShortUrl(), sendOrParseEx);
                    } else {
                        compensateMapper.incrementRetryById(row.getId());
                        failCnt++;
                        try {
                            Thread.sleep(100L); // Broker 持续挂时：防 200 条 1ms 打完空转打满日志 + CPU
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            }
        }

        if (totalHandled > 0) {
            log.info("统计补偿巡检完成，本轮处理{}条，成功={} 重试={} 永久失败={}",
                    totalHandled, successCnt, failCnt, permanentFailCnt);
        }
    }
}
