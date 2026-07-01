#include "gateway/ShardedRateLimiter.hpp"
#include <algorithm>

ShardedRateLimiter::ShardedRateLimiter(double global_tps, double fallback_fraction) {
    
    local_tps_limit_ = global_tps * fallback_fraction;
    
    max_burst_ = local_tps_limit_ / static_cast<double>(SHARD_COUNT);
    if (max_burst_ < 1.0) max_burst_ = 1.0;

    uint64_t now = GetCurrentTimeMillis();
    for (size_t i = 0; i < SHARD_COUNT; ++i) {
        shards_[i].tokens.store(max_burst_);
        shards_[i].last_refill_time.store(now);
    }
}

size_t ShardedRateLimiter::GetShardIndex(const std::string& tenant_id) const {
    size_t hash = 14695981039346656037ULL; 
    for (char ch : tenant_id) {
        hash ^= static_cast<size_t>(ch);
        hash *= 1099511628211ULL;
    }
    return hash % SHARD_COUNT;
}

uint64_t ShardedRateLimiter::GetCurrentTimeMillis() const {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count()
    );
}

bool ShardedRateLimiter::AllowRequest(const std::string& tenant_id) {
    size_t idx = GetShardIndex(tenant_id);
    auto& shard = shards_[idx];

    uint64_t now = GetCurrentTimeMillis();
    uint64_t last = shard.last_refill_time.load();

    
    if (now > last) {
        if (shard.last_refill_time.compare_exchange_strong(last, now)) {
            double elapsed_seconds = static_cast<double>(now - last) / 1000.0;
            
            double tokens_to_add = (local_tps_limit_ / static_cast<double>(SHARD_COUNT)) * elapsed_seconds;
            
            double current_tokens = shard.tokens.load();
            double new_tokens = std::min(max_burst_, current_tokens + tokens_to_add);
            shard.tokens.store(new_tokens);
        }
    }

    
    double current_tokens = shard.tokens.load();
    while (current_tokens >= 1.0) {
        if (shard.tokens.compare_exchange_weak(current_tokens, current_tokens - 1.0)) {
            return true; 
        }
        
    }

    return false; 
}