#ifndef SHARDED_RATE_LIMITER_HPP
#define SHARDED_RATE_LIMITER_HPP

#include <string>
#include <array>
#include <atomic>
#include <chrono>

class ShardedRateLimiter {
public:
    
    ShardedRateLimiter(double global_tps, double fallback_fraction);
    ~ShardedRateLimiter() = default;

    ShardedRateLimiter(const ShardedRateLimiter&) = delete;
    ShardedRateLimiter& operator=(const ShardedRateLimiter&) = delete;

    
    bool AllowRequest(const std::string& tenant_id);

private:
    struct LimiterBucket {
        std::atomic<double> tokens{0.0};
        std::atomic<uint64_t> last_refill_time{0}; 
    };

    static constexpr size_t SHARD_COUNT = 32;
    std::array<LimiterBucket, SHARD_COUNT> shards_;
    
    double local_tps_limit_;
    double max_burst_;

    size_t GetShardIndex(const std::string& tenant_id) const;
    uint64_t GetCurrentTimeMillis() const;
};

#endif 