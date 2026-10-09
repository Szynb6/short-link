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

package com.nageoffer.shortlink.project.mq.consumer;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.Week;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.nageoffer.shortlink.project.common.convention.exception.ServiceException;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessLogsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkBrowserStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkDeviceStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkLocaleStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkNetworkStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkOsStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkStatsTodayDO;
import com.nageoffer.shortlink.project.dao.entity.ShortLinkGotoDO;
import com.nageoffer.shortlink.project.dao.mapper.LinkAccessLogsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkAccessStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkBrowserStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkDeviceStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkLocaleStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkNetworkStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkOsStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkStatsTodayMapper;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkGotoMapper;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkMapper;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import com.nageoffer.shortlink.project.mq.idempotent.MessageQueueIdempotentHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.LOCK_GID_UPDATE_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_TOPIC_KEY;
import static com.nageoffer.shortlink.project.common.constant.ShortLinkConstant.AMAP_REMOTE_URL;

/**
 * 短链接监控状态保存消息队列消费者
 * 公众号：马丁玩编程，回复：加群，添加马哥微信（备注：link）获取项目资料
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = SHORT_LINK_STATS_TOPIC_KEY,
        consumerGroup = "short-link-stats-consumer-group",
        // 消费线程数，高于原 Redis Stream 单线程 batchSize=10 的吞吐
        consumeThreadNumber = 8,
        // 失败重试 16 次（10s/30s/1m...2h 退避），全部失败进死信 %DLQ%short-link-stats-consumer-group
        maxReconsumeTimes = 16)
public class ShortLinkStatsSaveConsumer implements RocketMQListener<MessageExt> {

    private final ShortLinkMapper shortLinkMapper;
    private final ShortLinkGotoMapper shortLinkGotoMapper;
    private final RedissonClient redissonClient;
    private final LinkAccessStatsMapper linkAccessStatsMapper;
    private final LinkLocaleStatsMapper linkLocaleStatsMapper;
    private final LinkOsStatsMapper linkOsStatsMapper;
    private final LinkBrowserStatsMapper linkBrowserStatsMapper;
    private final LinkAccessLogsMapper linkAccessLogsMapper;
    private final LinkDeviceStatsMapper linkDeviceStatsMapper;
    private final LinkNetworkStatsMapper linkNetworkStatsMapper;
    private final LinkStatsTodayMapper linkStatsTodayMapper;
    private final MessageQueueIdempotentHandler messageQueueIdempotentHandler;
    private final TransactionTemplate transactionTemplate;

    @Value("${short-link.stats.locale.amap-key}")
    private String statsLocaleAmapKey;

    @Override
    public void onMessage(MessageExt message) {
        String keys = message.getKeys();
        // 判断当前消息是否正在被消费
        if (messageQueueIdempotentHandler.isMessageBeingConsumed(keys)) {
            // 判断当前的这个消息流程是否执行完成，如果执行完成就直接返回，避免重复消费
            // 已经消费完成直接返回不需要额外消费,value为1表示被其他的消费者消费完成
            if (messageQueueIdempotentHandler.isAccomplish(keys)) {
                return;
            }
            // value存在但是为0表示当前消费者正在消费
            throw new ServiceException("消息未完成流程，需要消息队列重试");
        }
        // value不存在表示这条消息没有被消费过
        try {
            String messageBody = new String(message.getBody(), StandardCharsets.UTF_8);
            ShortLinkStatsRecordDTO statsRecord = JSON.parseObject(messageBody, ShortLinkStatsRecordDTO.class);
            actualSaveShortLinkStats(statsRecord);
        } catch (Throwable ex) {
            // 消费流程异常：删除幂等标识，抛出异常触发 RocketMQ 按延迟级别退避重试
            messageQueueIdempotentHandler.delMessageProcessed(keys);
            log.error("记录短链接监控消费异常", ex);
            throw new RuntimeException(ex);
        }
        // 消费成功将value设为1
        messageQueueIdempotentHandler.setAccomplish(keys);
    }

    public void actualSaveShortLinkStats(ShortLinkStatsRecordDTO statsRecord) {
        String fullShortUrl = statsRecord.getFullShortUrl();
        Date currentDate = statsRecord.getCurrentDate();
        // ① 锁外/事务外：调用高德地图 API 获取用户位置信息
        //    外网 HTTP 耗时 30~300ms，放在分布式锁和数据库事务之外，避免长时间占用锁和事务连接
        LocaleInfo localeInfo = queryLocaleInfo(statsRecord, fullShortUrl, currentDate);
        String actualProvince = localeInfo.province();
        String actualCity = localeInfo.city();
        LinkLocaleStatsDO localeStatsDO = localeInfo.localeStatsDO();
        // ② 锁内 + 事务内：查 gid 并写 8 张统计表
        //    readLock 与后台修改短链 gid 的 writeLock 互斥，防止计数器写错 gid 分组；
        //    TransactionTemplate 保证 8 张表任一失败全部回滚，重投后整条消息干净重来，不会重复累加
        RReadWriteLock readWriteLock = redissonClient.getReadWriteLock(String.format(LOCK_GID_UPDATE_KEY, fullShortUrl));
        RLock rLock = readWriteLock.readLock();
        rLock.lock();
        try {
            transactionTemplate.execute(status -> {
                LambdaQueryWrapper<ShortLinkGotoDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                        .eq(ShortLinkGotoDO::getFullShortUrl, fullShortUrl);
                // 通过短链接拿到分组id
                ShortLinkGotoDO shortLinkGotoDO = shortLinkGotoMapper.selectOne(queryWrapper);
                String gid = shortLinkGotoDO.getGid();
                // 获取当前的小时和星期
                int hour = DateUtil.hour(currentDate, true);
                Week week = DateUtil.dayOfWeekEnum(currentDate);
                int weekValue = week.getIso8601Value();
                LinkAccessStatsDO linkAccessStatsDO = LinkAccessStatsDO.builder()
                        .pv(1)
                        // 是否为新用户访问短链接，是则uv+1
                        .uv(statsRecord.getUvFirstFlag() ? 1 : 0)
                        // 是否为新用户访问短链接，是则uip+1
                        .uip(statsRecord.getUipFirstFlag() ? 1 : 0)
                        .hour(hour)
                        .weekday(weekValue)
                        .fullShortUrl(fullShortUrl)
                        .date(currentDate)
                        .build();
                linkAccessStatsMapper.shortLinkStats(linkAccessStatsDO);
                if (localeStatsDO != null) {
                    linkLocaleStatsMapper.shortLinkLocaleState(localeStatsDO);
                }
                linkOsStatsMapper.shortLinkOsState(LinkOsStatsDO.builder()
                        .os(statsRecord.getOs())
                        .cnt(1)
                        .fullShortUrl(fullShortUrl)
                        .date(currentDate)
                        .build());
                linkBrowserStatsMapper.shortLinkBrowserState(LinkBrowserStatsDO.builder()
                        .browser(statsRecord.getBrowser())
                        .cnt(1)
                        .fullShortUrl(fullShortUrl)
                        .date(currentDate)
                        .build());
                linkDeviceStatsMapper.shortLinkDeviceState(LinkDeviceStatsDO.builder()
                        .device(statsRecord.getDevice())
                        .cnt(1)
                        .fullShortUrl(fullShortUrl)
                        .date(currentDate)
                        .build());
                linkNetworkStatsMapper.shortLinkNetworkState(LinkNetworkStatsDO.builder()
                        .network(statsRecord.getNetwork())
                        .cnt(1)
                        .fullShortUrl(fullShortUrl)
                        .date(currentDate)
                        .build());
                // 访问日志记录
                linkAccessLogsMapper.insert(LinkAccessLogsDO.builder()
                        .user(statsRecord.getUv())
                        .ip(statsRecord.getRemoteAddr())
                        .browser(statsRecord.getBrowser())
                        .os(statsRecord.getOs())
                        .network(statsRecord.getNetwork())
                        .device(statsRecord.getDevice())
                        .locale(StrUtil.join("-", "中国", actualProvince, actualCity))
                        .fullShortUrl(fullShortUrl)
                        .build());
                shortLinkMapper.incrementStats(gid, fullShortUrl, 1, statsRecord.getUvFirstFlag() ? 1 : 0, statsRecord.getUipFirstFlag() ? 1 : 0);
                linkStatsTodayMapper.shortLinkTodayState(LinkStatsTodayDO.builder()
                        .todayPv(1)
                        .todayUv(statsRecord.getUvFirstFlag() ? 1 : 0)
                        .todayUip(statsRecord.getUipFirstFlag() ? 1 : 0)
                        .fullShortUrl(fullShortUrl)
                        .date(currentDate)
                        .build());
                return null;
            });
        } finally {
            rLock.unlock();
        }
    }

    /**
     * 高德 IP 定位结果：解析出的省市区 + 可选的省市区维度统计对象（定位失败时省市区为"未知"，localeStatsDO 为 null）
     */
    private record LocaleInfo(String province, String city, LinkLocaleStatsDO localeStatsDO) {
    }

    /**
     * 锁外/事务外调用高德 IP 定位 API，失败不抛异常，省市区兜底为"未知"
     */
    private LocaleInfo queryLocaleInfo(ShortLinkStatsRecordDTO statsRecord, String fullShortUrl, Date currentDate) {
        String province = "未知";
        String city = "未知";
        LinkLocaleStatsDO localeStatsDO = null;
        if (StrUtil.isNotBlank(statsRecord.getRemoteAddr())) {
            try {
                Map<String, Object> localeParamMap = new HashMap<>();
                localeParamMap.put("key", statsLocaleAmapKey);
                localeParamMap.put("ip", statsRecord.getRemoteAddr());
                String localeResultStr = HttpUtil.get(AMAP_REMOTE_URL, localeParamMap);
                JSONObject localeResultObj = JSON.parseObject(localeResultStr);
                String infoCode = localeResultObj.getString("infocode");
                if (StrUtil.isNotBlank(infoCode) && StrUtil.equals(infoCode, "10000")) {
                    String amapProvince = localeResultObj.getString("province");
                    boolean unknownFlag = StrUtil.equals(amapProvince, "[]");
                    province = unknownFlag ? province : amapProvince;
                    city = unknownFlag ? city : localeResultObj.getString("city");
                    if (!unknownFlag) {
                        localeStatsDO = LinkLocaleStatsDO.builder()
                                .province(province)
                                .city(city)
                                .adcode(localeResultObj.getString("adcode"))
                                .cnt(1)
                                .fullShortUrl(fullShortUrl)
                                .country("中国")
                                .date(currentDate)
                                .build();
                    }
                }
            } catch (Exception ex) {
                // 高德定位失败只影响省市区维度，不阻塞 PV/UV 主统计，省市区用"未知"兜底
                log.warn("高德定位失败，省市区使用未知兜底，ip={}", statsRecord.getRemoteAddr(), ex);
            }
        }
        return new LocaleInfo(province, city, localeStatsDO);
    }
}
