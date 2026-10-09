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

package com.nageoffer.shortlink.project.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.nageoffer.shortlink.project.dao.entity.LinkStatsCompensateDO;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

/**
 * 短链接统计 MQ 失败补偿持久层
 */
public interface LinkStatsCompensateMapper extends BaseMapper<LinkStatsCompensateDO> {

    /**
     * 幂等插入一条待补偿记录（msg_keys 重复则忽略，防止 Netty 异常回调多次触发重复补偿）
     * BaseDO 三个公共字段（create_time/update_time/del_flag）由 MyBatis-Plus FieldFill 自动注入
     */
    @Insert("INSERT IGNORE INTO t_link_stats_compensate " +
            "(msg_keys, full_short_url, payload_json, retry_cnt, status, create_time, update_time, del_flag) " +
            "VALUES " +
            "(#{do.msgKeys}, #{do.fullShortUrl}, #{do.payloadJson}, 0, 0, NOW(), NOW(), 0)")
    void insertIgnore(@Param("do") LinkStatsCompensateDO entity);

    /**
     * 扫描一批待补偿记录：status=0 待补偿 + 创建时间 > 60s 宽限期（让现场 asyncSend 先有机会自己成功）
     * 按 id 升序，取最老的 N 条，避免饿死旧记录
     */
    @Select("SELECT id, msg_keys, full_short_url, payload_json, retry_cnt, status, create_time, update_time, del_flag " +
            "FROM t_link_stats_compensate " +
            "WHERE status = 0 AND del_flag = 0 AND create_time < DATE_SUB(NOW(), INTERVAL 1 MINUTE) " +
            "ORDER BY id ASC LIMIT #{limit}")
    List<LinkStatsCompensateDO> selectPendingBatch(@Param("limit") int limit);

    /**
     * 补偿成功，更新状态为 1
     */
    @Update("UPDATE t_link_stats_compensate SET status = 1, update_time = NOW() WHERE id = #{id}")
    int updateSuccessById(@Param("id") Long id);

    /**
     * 本轮补偿失败，retry_cnt + 1
     */
    @Update("UPDATE t_link_stats_compensate SET retry_cnt = retry_cnt + 1, update_time = NOW() WHERE id = #{id}")
    int incrementRetryById(@Param("id") Long id);

    /**
     * 标记永久失败（超过最大重试次数，留给人工处理）
     */
    @Update("UPDATE t_link_stats_compensate SET status = 2, update_time = NOW() WHERE id = #{id}")
    int markPermanentFailById(@Param("id") Long id);
}
