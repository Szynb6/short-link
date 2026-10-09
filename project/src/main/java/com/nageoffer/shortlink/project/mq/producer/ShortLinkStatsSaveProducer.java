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

package com.nageoffer.shortlink.project.mq.producer;

import cn.hutool.core.lang.UUID;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nageoffer.shortlink.project.dao.entity.LinkStatsCompensateDO;
import com.nageoffer.shortlink.project.dao.mapper.LinkStatsCompensateMapper;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_TOPIC_KEY;
import static org.apache.rocketmq.spring.support.RocketMQHeaders.KEYS;

/**
 * 短链接监控状态保存消息队列生产者
 * 发送策略：RocketMQ asyncSend 异步发送（Tomcat 线程零等待）
 * 失败兜底：onException 写入 MySQL 补偿表 t_link_stats_compensate，由定时任务每分钟重投
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ShortLinkStatsSaveProducer {

    private static final long SEND_TIMEOUT_MS = 500L;

    private final RocketMQTemplate rocketMQTemplate;
    private final LinkStatsCompensateMapper compensateMapper;
    private final ObjectMapper objectMapper;

    /**
     * 异步发送短链接统计消息
     * 1. KEYS 头作为消费端幂等消息 ID（消费者 setIfAbsent 防重复写库）
     * 2. asyncSend 立即返回，Tomcat 线程不等 Broker ACK
     * 3. 发送异常 -> 落 MySQL 补偿表，定时任务重投
     */
    public void send(ShortLinkStatsRecordDTO statsRecord) {
        String keys = UUID.fastUUID().toString();
        Message<ShortLinkStatsRecordDTO> message = MessageBuilder
                .withPayload(statsRecord)
                .setHeader(KEYS, keys)
                .build();
        try {
            rocketMQTemplate.asyncSend(SHORT_LINK_STATS_TOPIC_KEY, message, new SendCallback() {
                // 回调执行在 NettyClientNIOWorker-x 线程上，任何重阻塞操作都要避免
                @Override
                public void onSuccess(SendResult sendResult) {
                    // 成功什么都不做，立刻释放 Netty Worker
                }

                @Override
                public void onException(Throwable e) {
                    log.warn("短链统计 asyncSend 失败，转 MySQL 补偿，keys={} fullShortUrl={}",
                            keys, statsRecord.getFullShortUrl(), e);
                    saveCompensateSafely(keys, statsRecord);
                }
            }, SEND_TIMEOUT_MS);
        } catch (Exception e) {
            // asyncSend 提交阶段就异常（比如 topic 不存在 / name-server 连不上、消息体太大等）
            // 这时 SendCallback 还没注册上，根本进不了 onException，必须在这里二次兜底
            log.warn("短链统计 asyncSend 提交失败，转 MySQL 补偿，keys={}", keys, e);
            saveCompensateSafely(keys, statsRecord);
        }
    }

    /**
     * 把失败记录落补偿表，双保险 catch 防止「补偿表落库也失败」的异常冒泡到 Netty Worker / Tomcat 线程
     * （Netty Worker 线程抛异常会关闭连接，Tomcat 线程抛异常会影响跳转 sendRedirect）
     */
    private void saveCompensateSafely(String keys, ShortLinkStatsRecordDTO statsRecord) {
        try {
            String payloadJson;
            try {
                payloadJson = objectMapper.writeValueAsString(statsRecord);
            } catch (JsonProcessingException jsonEx) {
                // JSON 序列化极端失败：退化成手拼的关键字段 JSON，至少保证能人工补数据
                payloadJson = "{\"fallback\":true,\"fullShortUrl\":\"" + statsRecord.getFullShortUrl() + "\",\"keys\":\"" + keys + "\"}";
            }
            LinkStatsCompensateDO entity = LinkStatsCompensateDO.builder()
                    .msgKeys(keys)
                    .fullShortUrl(statsRecord.getFullShortUrl())
                    .payloadJson(payloadJson)
                    .build();
            compensateMapper.insertIgnore(entity);
        } catch (Throwable t) {
            // 最后防线：MySQL 补偿表也写不进去（DB 挂了/连接池满）→ 独立 ERROR 日志，人工排查
            log.error("COMPENSATE_DB_FAIL 记录人工补: keys={} fullShortUrl={} remoteAddr={} uv={}",
                    keys, statsRecord.getFullShortUrl(), statsRecord.getRemoteAddr(), statsRecord.getUv(), t);
        }
    }
}
