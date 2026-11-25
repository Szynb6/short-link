package com.nageoffer.shortlink.project.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.nageoffer.shortlink.project.dao.entity.LinkStatsConsumeLogDO;

/**
 * 短链接统计 MQ 消费幂等持久层
 */
public interface LinkStatsConsumeLogMapper extends BaseMapper<LinkStatsConsumeLogDO> {
}
