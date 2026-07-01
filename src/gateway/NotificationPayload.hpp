#ifndef NOTIFICATION_PAYLOAD_HPP
#define NOTIFICATION_PAYLOAD_HPP

#include <string>
#include <chrono>
#include <cstdint>

enum class ErrorCategory {
    NONE,
    TRANSIENT_HTTP_5XX,
    RATE_LIMITED,
    INVALID_PAYLOAD,
    PROVIDER_DOWN
};

struct NotificationPayload {
    std::string message_id;
    std::string tenant_id;
    std::string recipient;
    std::string channel; 
    std::string body;
    int32_t retry_count{0};

    
    std::chrono::steady_clock::time_point enqueued_at{std::chrono::steady_clock::now()};
};

struct CacheEntry {
    enum class Status { PENDING, SUCCESS, FAILED };
    
    Status status{Status::PENDING};
    std::string tenant_id;
    std::chrono::steady_clock::time_point expiration;
    std::string response_body;
};

#endif