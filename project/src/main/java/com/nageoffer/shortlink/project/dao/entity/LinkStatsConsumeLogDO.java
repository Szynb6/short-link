package com.nageoffer.shortlink.project.dao.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.nageoffer.shortlink.project.common.database.BaseDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 短链接统计 MQ 消费幂等持久层对象
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("t_link_stats_consume_log")
public class LinkStatsConsumeLogDO extends BaseDO {

    /**
     * id
     */
    private Long id;

    /**
     * RocketMQ KEYS 幂等唯一ID（UUID）
     */
    private String msgKeys;

    /**
     * 完整短链接
     */
    private String fullShortUrl;
}
