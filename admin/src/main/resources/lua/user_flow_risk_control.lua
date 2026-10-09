-- 设置用户访问频率限制的参数
local username = KEYS[1]
local timeWindow = tonumber(ARGV[1]) -- 时间窗口，单位：秒
local maxAccessCount = tonumber(ARGV[2]) -- 时间窗口内最大访问次数
local currentTimeMillis = tonumber(ARGV[3]) -- 当前时间戳，单位：毫秒
local uniqueId = ARGV[4] -- 本次请求唯一标识，保证 ZSET member 唯一

-- 构造 Redis 中存储用户访问记录的键名（ZSET，score 为请求时间戳）
local accessKey = "short-link:user-flow-risk-control:" .. username

-- 移除窗口之外的旧访问记录
redis.call("ZREMRANGEBYSCORE", accessKey, 0, currentTimeMillis - timeWindow * 1000)

-- 统计当前时间窗口内的真实访问次数
local currentAccessCount = redis.call("ZCARD", accessKey)

-- 超过限制则拒绝
if currentAccessCount >= maxAccessCount then
    return 0
end

-- 记录本次访问（member 由时间戳和唯一标识拼接，保证唯一性）
redis.call("ZADD", accessKey, currentTimeMillis, tostring(currentTimeMillis) .. ":" .. tostring(uniqueId))

-- 设置键的过期时间，避免遗留数据占用内存
redis.call("PEXPIRE", accessKey, timeWindow * 1000)

-- 返回 1 表示放行
return 1
