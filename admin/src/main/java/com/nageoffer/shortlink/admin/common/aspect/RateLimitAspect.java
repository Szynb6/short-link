package com.nageoffer.shortlink.admin.common.aspect;

import cn.hutool.core.util.StrUtil;
import com.nageoffer.shortlink.admin.common.annotation.RateLimit;
import com.nageoffer.shortlink.admin.common.biz.user.UserContext;
import com.nageoffer.shortlink.admin.common.convention.exception.ClientException;
import com.nageoffer.shortlink.admin.common.enums.LimitDimension;
import com.nageoffer.shortlink.admin.config.UserFlowRiskControlConfiguration;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.servlet.HandlerMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.nageoffer.shortlink.admin.common.convention.errorcode.BaseErrorCode.FLOW_LIMIT_ERROR;

/**
 * 基于 ZSET + Lua 滑动窗口的多维限流 AOP 切面
 */
@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class RateLimitAspect {

    private final StringRedisTemplate stringRedisTemplate;
    private final UserFlowRiskControlConfiguration userFlowRiskControlConfiguration;

    private static final String RATE_LIMIT_KEY_PREFIX = "short-link:rate-limit:";
    private static final DefaultRedisScript<Long> MULTI_DIM_RATE_LIMIT_SCRIPT;

    static {
        MULTI_DIM_RATE_LIMIT_SCRIPT = new DefaultRedisScript<>();
        MULTI_DIM_RATE_LIMIT_SCRIPT.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/multi_dim_rate_limit.lua")));
        MULTI_DIM_RATE_LIMIT_SCRIPT.setResultType(Long.class);
    }

    @Around("@annotation(rateLimit)")
    public Object around(ProceedingJoinPoint joinPoint, RateLimit rateLimit) throws Throwable {
        if (Boolean.FALSE.equals(userFlowRiskControlConfiguration.getEnable())) {
            return joinPoint.proceed();
        }
        RateLimit.Rule[] rules = rateLimit.value();
        if (rules == null || rules.length == 0) {
            return joinPoint.proceed();
        }
        RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
        if (!(requestAttributes instanceof ServletRequestAttributes servletRequestAttributes)) {
            return joinPoint.proceed();
        }
        HttpServletRequest request = servletRequestAttributes.getRequest();

        List<String> keys = new ArrayList<>(rules.length);
        List<String> args = new ArrayList<>(2 + rules.length * 2);
        args.add(String.valueOf(System.currentTimeMillis()));
        args.add(UUID.randomUUID().toString());

        for (RateLimit.Rule rule : rules) {
            String key = buildDimensionKey(rule.dimension(), request);
            if (key == null) {
                continue;
            }
            keys.add(key);
            args.add(String.valueOf(rule.timeWindow()));
            args.add(String.valueOf(rule.maxCount()));
        }

        if (!keys.isEmpty()) {
            Long result;
            try {
                result = stringRedisTemplate.execute(MULTI_DIM_RATE_LIMIT_SCRIPT, keys, args.toArray());
            } catch (Throwable ex) {
                log.error("执行多维滑动窗口限流 Lua 脚本出错", ex);
                throw new ClientException(FLOW_LIMIT_ERROR);
            }
            if (result == null || result == 0L) {
                throw new ClientException(FLOW_LIMIT_ERROR);
            }
        }
        return joinPoint.proceed();
    }

    private String buildDimensionKey(LimitDimension dimension, HttpServletRequest request) {
        return switch (dimension) {
            case GLOBAL -> RATE_LIMIT_KEY_PREFIX + "global";
            case API -> {
                String pattern = (String) request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
                String path = StrUtil.isNotBlank(pattern) ? pattern : request.getRequestURI();
                yield RATE_LIMIT_KEY_PREFIX + "api:" + request.getMethod() + ":" + path;
            }
            case USER -> {
                String username = UserContext.getUsername();
                yield StrUtil.isNotBlank(username) ? RATE_LIMIT_KEY_PREFIX + "user:" + username : null;
            }
            case IP -> RATE_LIMIT_KEY_PREFIX + "ip:" + getActualIp(request);
        };
    }

    private String getActualIp(HttpServletRequest request) {
        String ipAddress = request.getHeader("X-Forwarded-For");
        if (StrUtil.isBlank(ipAddress) || "unknown".equalsIgnoreCase(ipAddress)) {
            ipAddress = request.getHeader("Proxy-Client-IP");
        }
        if (StrUtil.isBlank(ipAddress) || "unknown".equalsIgnoreCase(ipAddress)) {
            ipAddress = request.getHeader("WL-Proxy-Client-IP");
        }
        if (StrUtil.isBlank(ipAddress) || "unknown".equalsIgnoreCase(ipAddress)) {
            ipAddress = request.getHeader("HTTP_CLIENT_IP");
        }
        if (StrUtil.isBlank(ipAddress) || "unknown".equalsIgnoreCase(ipAddress)) {
            ipAddress = request.getHeader("HTTP_X_FORWARDED_FOR");
        }
        if (StrUtil.isBlank(ipAddress) || "unknown".equalsIgnoreCase(ipAddress)) {
            ipAddress = request.getRemoteAddr();
        }
        if (StrUtil.isNotBlank(ipAddress) && ipAddress.contains(",")) {
            ipAddress = ipAddress.split(",")[0].trim();
        }
        return ipAddress;
    }
}
