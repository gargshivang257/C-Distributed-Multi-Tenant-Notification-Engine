#ifndef IDEMPOTENCY_ENGINE_HPP
#define IDEMPOTENCY_ENGINE_HPP

#include "gateway/NotificationPayload.hpp"
#include <string>
#include <mutex>
#include <optional>
#include <chrono>
#include <hiredis/hiredis.h> 

class IdempotencyEngine {
public:
    
    explicit IdempotencyEngine(std::chrono::seconds entry_ttl, 
                               const std::string& redis_host = "127.0.0.1", 
                               int redis_port = 6379);
    ~IdempotencyEngine();

    
    IdempotencyEngine(const IdempotencyEngine&) = delete;
    IdempotencyEngine& operator=(const IdempotencyEngine&) = delete;
    IdempotencyEngine(IdempotencyEngine&&) = delete;
    IdempotencyEngine& operator=(IdempotencyEngine&&) = delete;

    std::string ComputeSha256(const std::string& input) const;

   
    bool TryInsert(const std::string& key, const std::string& tenant_id);
    
    
    std::optional<std::string> GetCachedResponse(const std::string& key);
    
   
    void UpdateStatus(const std::string& key, const std::string& response_body);

private:
    redisContext* redis_ctx_;    
    std::mutex engine_mutex_;    
    std::chrono::seconds ttl_;   
};

#endif 