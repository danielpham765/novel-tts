from __future__ import annotations

# NOTE: These scripts intentionally use Redis TIME so that rolling windows are consistent
# across workers and the supervisor.

TRY_GRANT_LUA = r"""
-- KEYS:
--  1 tpm_freezed_zset
--  2 tpm_freezed_hash
--  3 tpm_locked_zset
--  4 tpm_locked_hash
--  5 rpm_freezed_zset
--  6 rpm_locked_zset
--  7 rpd_freezed_zset
--  8 rpd_locked_zset
--
-- ARGV:
--  1 rpm_limit
--  2 tpm_limit
--  3 rpd_limit
--  4 requested_tokens
--  5 request_id

local rpm_limit = tonumber(ARGV[1]) or 0
local tpm_limit = tonumber(ARGV[2]) or 0
local rpd_limit = tonumber(ARGV[3]) or 0
local requested_tokens = tonumber(ARGV[4]) or 0
local request_id = tostring(ARGV[5] or "")

local t = redis.call("TIME")
local now = tonumber(t[1]) + (tonumber(t[2]) / 1000000.0)

local function cleanup_window(zset_key, hash_key, window_seconds)
  local window_start = now - window_seconds
  local stale = redis.call("ZRANGEBYSCORE", zset_key, "-inf", window_start)
  if stale and #stale > 0 then
    redis.call("ZREM", zset_key, unpack(stale))
    if hash_key and hash_key ~= "" then
      redis.call("HDEL", hash_key, unpack(stale))
    end
  end
  return window_start
end

-- Cleanup.
local tpm_window_start = cleanup_window(KEYS[1], KEYS[2], 60)
cleanup_window(KEYS[3], KEYS[4], 60)
local rpm_window_start = cleanup_window(KEYS[5], "", 60)
cleanup_window(KEYS[6], "", 60)
local rpd_window_start = cleanup_window(KEYS[7], "", 86400)
cleanup_window(KEYS[8], "", 86400)

-- Count RPM usage (freezed + locked).
local rpm_used = 0
if rpm_limit > 0 then
  rpm_used = tonumber(redis.call("ZCOUNT", KEYS[5], rpm_window_start, "+inf")) + tonumber(redis.call("ZCOUNT", KEYS[6], rpm_window_start, "+inf"))
end

-- Count RPD usage (freezed + locked).
local rpd_used = 0
if rpd_limit > 0 then
  rpd_used = tonumber(redis.call("ZCOUNT", KEYS[7], rpd_window_start, "+inf")) + tonumber(redis.call("ZCOUNT", KEYS[8], rpd_window_start, "+inf"))
end

-- Sum TPM usage (freezed + locked) for active members in the last 60 seconds.
local function sum_tokens(zset_key, hash_key, window_start)
  local members = redis.call("ZRANGEBYSCORE", zset_key, window_start, "+inf")
  if not members or #members == 0 then
    return 0, {}
  end
  local vals = redis.call("HMGET", hash_key, unpack(members))
  local total = 0
  local token_map = {}
  for i = 1, #members do
    local raw = vals[i]
    local tok = tonumber(raw) or 0
    if tok > 0 then
      total = total + tok
      token_map[members[i]] = tok
    end
  end
  return total, token_map
end

local tpm_used = 0
local tpm_items = {}
if tpm_limit > 0 then
  local freezed_sum, freezed_map = sum_tokens(KEYS[1], KEYS[2], tpm_window_start)
  local locked_sum, locked_map = sum_tokens(KEYS[3], KEYS[4], tpm_window_start)
  tpm_used = freezed_sum + locked_sum
  for member, tok in pairs(freezed_map) do
    table.insert(tpm_items, {member = member, score = tonumber(redis.call("ZSCORE", KEYS[1], member)) or now, tok = tok})
  end
  for member, tok in pairs(locked_map) do
    table.insert(tpm_items, {member = member, score = tonumber(redis.call("ZSCORE", KEYS[3], member)) or now, tok = tok})
  end
end

local allow_rpm = (rpm_limit <= 0) or (rpm_used < rpm_limit)
local allow_rpd = (rpd_limit <= 0) or (rpd_used < rpd_limit)
local allow_tpm = (tpm_limit <= 0) or ((tpm_used + requested_tokens) <= tpm_limit)

local function compute_wait_rpm()
  if rpm_limit <= 0 then
    return 0
  end
  if rpm_used < rpm_limit then
    return 0
  end
  local scores = {}
  local a = redis.call("ZRANGEBYSCORE", KEYS[5], rpm_window_start, "+inf", "WITHSCORES")
  for i = 2, #a, 2 do
    table.insert(scores, tonumber(a[i]) or now)
  end
  local b = redis.call("ZRANGEBYSCORE", KEYS[6], rpm_window_start, "+inf", "WITHSCORES")
  for i = 2, #b, 2 do
    table.insert(scores, tonumber(b[i]) or now)
  end
  table.sort(scores)
  local need_drop = rpm_used - (rpm_limit - 1)
  local idx = math.max(1, math.min(#scores, need_drop))
  local cutoff = scores[idx] or now
  local expiry = cutoff + 60.0
  local wait = expiry - now + 0.05
  if wait < 0.25 then
    wait = 0.25
  end
  return wait
end

local function compute_wait_rpd()
  if rpd_limit <= 0 then
    return 0
  end
  if rpd_used < rpd_limit then
    return 0
  end
  -- Approximation: wait for the oldest member in either set to expire.
  local oldest = nil
  local a = redis.call("ZRANGEBYSCORE", KEYS[7], rpd_window_start, "+inf", "WITHSCORES", "LIMIT", 0, 1)
  if a and #a >= 2 then
    oldest = tonumber(a[2]) or oldest
  end
  local b = redis.call("ZRANGEBYSCORE", KEYS[8], rpd_window_start, "+inf", "WITHSCORES", "LIMIT", 0, 1)
  if b and #b >= 2 then
    local val = tonumber(b[2]) or nil
    if val and (not oldest or val < oldest) then
      oldest = val
    end
  end
  if not oldest then
    return 60.0
  end
  local expiry = oldest + 86400.0
  local wait = expiry - now + 0.05
  if wait < 1.0 then
    wait = 1.0
  end
  return wait
end

local function compute_wait_tpm()
  if tpm_limit <= 0 then
    return 0
  end
  if (tpm_used + requested_tokens) <= tpm_limit then
    return 0
  end
  -- Sort by score (oldest first) and accumulate tokens that would expire.
  table.sort(tpm_items, function(a, b) return (a.score or now) < (b.score or now) end)
  local need_reduce = (tpm_used + requested_tokens) - tpm_limit
  local reduced = 0
  local cutoff = nil
  for i = 1, #tpm_items do
    reduced = reduced + (tonumber(tpm_items[i].tok) or 0)
    if reduced >= need_reduce then
      cutoff = tonumber(tpm_items[i].score) or now
      break
    end
  end
  if not cutoff then
    cutoff = (tpm_items[1] and tonumber(tpm_items[1].score)) or now
  end
  local expiry = cutoff + 60.0
  local wait = expiry - now + 0.05
  if wait < 0.25 then
    wait = 0.25
  end
  return wait
end

local rpm_would = rpm_used + 1
local tpm_would = tpm_used + requested_tokens
local rpd_would = rpd_used + 1

if allow_rpm and allow_rpd and allow_tpm then
  if request_id == "" then
    request_id = tostring(math.floor(now * 1000000))
  end
  local grant_id = string.format("%.6f:%s", now, request_id)
  if rpm_limit > 0 then
    redis.call("ZADD", KEYS[5], now, grant_id)
  end
  if rpd_limit > 0 then
    redis.call("ZADD", KEYS[7], now, grant_id)
  end
  if tpm_limit > 0 then
    redis.call("ZADD", KEYS[1], now, grant_id)
    redis.call("HSET", KEYS[2], grant_id, requested_tokens)
  end
  return {
    1,
    grant_id,
    tostring(now),
    "",
    tostring(rpm_used),
    tostring(rpm_limit),
    tostring(tpm_used),
    tostring(tpm_limit),
    tostring(rpd_used),
    tostring(rpd_limit),
    tostring(requested_tokens),
    tostring(rpm_would),
    tostring(tpm_would),
    tostring(rpd_would)
  }
end

local wait_rpm = compute_wait_rpm()
local wait_tpm = compute_wait_tpm()
local wait_rpd = compute_wait_rpd()
local wait = wait_rpm
if wait_tpm > wait then
  wait = wait_tpm
end
if wait_rpd > wait then
  wait = wait_rpd
end
if wait < 0.25 then
  wait = 0.25
end
local reasons = {}
if wait_rpm > 0 then table.insert(reasons, "RPM") end
if wait_tpm > 0 then table.insert(reasons, "TPM") end
if wait_rpd > 0 then table.insert(reasons, "RPD") end
local reason_text = table.concat(reasons, ",")
return {
  0,
  "",
  tostring(wait),
  reason_text,
  tostring(rpm_used),
  tostring(rpm_limit),
  tostring(tpm_used),
  tostring(tpm_limit),
  tostring(rpd_used),
  tostring(rpd_limit),
  tostring(requested_tokens),
  tostring(rpm_would),
  tostring(tpm_would),
  tostring(rpd_would)
}
"""


COMMIT_LUA = r"""
-- KEYS:
--  1 tpm_freezed_zset
--  2 tpm_freezed_hash
--  3 tpm_locked_zset
--  4 tpm_locked_hash
--  5 rpm_freezed_zset
--  6 rpm_locked_zset
--  7 rpd_freezed_zset
--  8 rpd_locked_zset
--
-- ARGV:
--  1 grant_id
--  2 outcome ("success"|"fail")
--  3 tokens_fallback
--  4 tokens_used (optional, overrides on success)

local grant_id = tostring(ARGV[1] or "")
local outcome = tostring(ARGV[2] or "")
local tokens_fallback = tonumber(ARGV[3]) or 0
local tokens_used = tonumber(ARGV[4]) or 0

local t = redis.call("TIME")
local now = tonumber(t[1]) + (tonumber(t[2]) / 1000000.0)

if grant_id == "" then
  return {0, tostring(now)}
end

-- RPM: always move to locked @t2 (success or fail).
redis.call("ZREM", KEYS[5], grant_id)
redis.call("ZADD", KEYS[6], now, grant_id)

-- RPD: always move to locked @t2 (success or fail).
redis.call("ZREM", KEYS[7], grant_id)
redis.call("ZADD", KEYS[8], now, grant_id)

-- TPM: remove freezed always; only lock on success.
local tok_raw = redis.call("HGET", KEYS[2], grant_id)
local tok = tonumber(tok_raw) or tokens_fallback
redis.call("ZREM", KEYS[1], grant_id)
redis.call("HDEL", KEYS[2], grant_id)

if outcome == "success" then
  if tokens_used and tokens_used > 0 then
    tok = tokens_used
  end
  local already = redis.call("ZSCORE", KEYS[3], grant_id)
  if not already then
    redis.call("ZADD", KEYS[3], now, grant_id)
    if tok and tok > 0 then
      redis.call("HSET", KEYS[4], grant_id, tok)
    end
  end
end

return {1, tostring(now)}
"""
