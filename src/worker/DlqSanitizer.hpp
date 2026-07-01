#ifndef DLQ_SANITIZER_HPP
#define DLQ_SANITIZER_HPP

#include "gateway/NotificationPayload.hpp"
#include <string>
#include <sstream>

class DlqSanitizer {
public:
    
    static std::string SerializeSanitized(const NotificationPayload& payload, const std::string& error_enum) {
        std::stringstream json;
        json << "{\n"
             << "  \"failed_message_id\": \"" << payload.message_id << "\",\n"
             << "  \"tenant_id\": \"" << payload.tenant_id << "\",\n"
             << "  \"error_category\": \"" << error_enum << "\",\n"
             << "  \"retry_count\": " << payload.retry_count << "\n"
             << "}";
        return json.str();
    }
};

#endif 