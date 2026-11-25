-- 多维滑动窗口限流 Lua 脚本（两阶段原子执行）
-- KEYS[1..N]: 本次叠加的限流维度 Key（global / api / user / ip）
-- ARGV[1]: 当前毫秒时间戳
-- ARGV[2]: 本次请求唯一标识（UUID）
-- ARGV[3..2N+2]: 成对传入每个维度的 timeWindow(秒) 与 maxAccessCount

local currentTimeMillis = tonumber(ARGV[1])
local uniqueId = ARGV[2]

-- 第一阶段：全量清理窗口外旧记录并校验次数（只读不写，任一维度超限立即拒绝）
for i = 1, #KEYS do
    local accessKey = KEYS[i]
    local timeWindow = tonumber(ARGV[2 * i + 1])
    local maxAccessCount = tonumber(ARGV[2 * i + 2])

    redis.call("ZREMRANGEBYSCORE", accessKey, 0, currentTimeMillis - timeWindow * 1000)
    local currentAccessCount = redis.call("ZCARD", accessKey)

    if currentAccessCount >= maxAccessCount then
        return 0
    end
end

-- 第二阶段：所有维度校验通过后统一记账并刷新过期时间
local member = tostring(currentTimeMillis) .. ":" .. tostring(uniqueId)
for i = 1, #KEYS do
    local accessKey = KEYS[i]
    local timeWindow = tonumber(ARGV[2 * i + 1])

    redis.call("ZADD", accessKey, currentTimeMillis, member)
    redis.call("PEXPIRE", accessKey, timeWindow * 1000)
end

return 1
