package com.nageoffer.shortlink.admin.common.enums;

/**
 * 限流维度枚举
 */
public enum LimitDimension {

    /**
     * 全局限流
     */
    GLOBAL,

    /**
     * 接口维度限流
     */
    API,

    /**
     * 用户维度限流
     */
    USER,

    /**
     * IP 维度限流
     */
    IP
}
