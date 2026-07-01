#ifndef RABBIT_CLIENT_HPP
#define RABBIT_CLIENT_HPP

#include "gateway/NotificationPayload.hpp"
#include <string>
#include <optional>
#include <mutex>

class RabbitClient {
public:
    RabbitClient(const std::string& connection_string);
    ~RabbitClient();

    
    RabbitClient(const RabbitClient&) = delete;
    RabbitClient& operator=(const RabbitClient&) = delete;

    
    bool PublishMessage(const std::string& queue_name, const NotificationPayload& payload);

    
    std::optional<NotificationPayload> ConsumeMessage(const std::string& queue_name);

private:
    std::string connection_uri_;
    std::mutex connection_mutex_;
    bool is_connected_{false};

    bool InitializeMockConnection();
};

#endif 