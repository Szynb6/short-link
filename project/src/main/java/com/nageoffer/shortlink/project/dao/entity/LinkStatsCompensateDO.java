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

package com.nageoffer.shortlink.project.dao.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.nageoffer.shortlink.project.common.database.BaseDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 短链接统计 MQ 失败补偿持久层对象
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("t_link_stats_compensate")
public class LinkStatsCompensateDO extends BaseDO {

    /**
     * id
     */
    private Long id;

    /**
     * RocketMQ KEYS 幂等唯一ID（UUID）
     */
    private String msgKeys;

    /**
     * 完整短链接（快速检索用，不用解析JSON）
     */
    private String fullShortUrl;

    /**
     * ShortLinkStatsRecordDTO 完整 JSON 序列化
     */
    private String payloadJson;

    /**
     * 已补偿重试次数
     */
    private Integer retryCnt;

    /**
     * 0待补偿 1成功 2永久失败
     */
    private Integer status;
}
