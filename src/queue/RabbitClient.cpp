#include "queue/RabbitClient.hpp"
#include "gateway/Logger.hpp"
#include <iostream>
#include <sstream>

RabbitClient::RabbitClient(const std::string& connection_string) 
    : connection_uri_(connection_string) {
    is_connected_ = InitializeMockConnection();
}

RabbitClient::~RabbitClient() {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    is_connected_ = false;
}

bool RabbitClient::InitializeMockConnection() {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    return !connection_uri_.empty();
}

bool RabbitClient::PublishMessage(const std::string& queue_name, const NotificationPayload& payload) {
    if (!is_connected_) return false;

    std::stringstream serialized_stream;
    serialized_stream << payload.message_id << "|"
                      << payload.tenant_id << "|"
                      << payload.recipient << "|"
                      << payload.channel << "|"
                      << payload.body;

    std::stringstream log_builder;
    log_builder << "[AMQP PUBLISH] Targeted Queue: " << queue_name 
                << " | Message ID: " << payload.message_id;

    SafeLogger::Instance().LogLine(log_builder.str());
              
    return true;
}

std::optional<NotificationPayload> RabbitClient::ConsumeMessage(const std::string& queue_name) {
    if (!is_connected_) return std::nullopt;

    
    static auto start_time = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time
    ).count();

    
    if (elapsed > 1500) {
        return std::nullopt;
    }

    static std::atomic<int> simulation_counter{0};
    int current_count = ++simulation_counter;

    
    if (current_count % 5 == 0) {
        NotificationPayload mock_payload{
            "msg-uuid-9999",
            "tenant-alpha",
            "+15555550123",
            "SMS",
            "Deterministic test content payload standard incoming data chunk",
            0,
            std::chrono::steady_clock::now()
        };

        std::stringstream log_builder;
        log_builder << "[AMQP CONSUME] Pulled from: " << queue_name;
        SafeLogger::Instance().LogLine(log_builder.str());

        return mock_payload;
    }

    
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    return std::nullopt;
}