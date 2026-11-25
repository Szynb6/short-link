package com.nageoffer.shortlink.admin.common.annotation;

import com.nageoffer.shortlink.admin.common.enums.LimitDimension;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 多维滑动窗口限流注解，支持单接口叠加多条限流规则
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RateLimit {

    /**
     * 叠加的限流规则列表
     */
    Rule[] value();

    @interface Rule {

        /**
         * 限流维度
         */
        LimitDimension dimension();

        /**
         * 时间窗口（单位：秒）
         */
        long timeWindow() default 1L;

        /**
         * 时间窗口内最大允许访问次数
         */
        long maxCount() default 20L;
    }
}
