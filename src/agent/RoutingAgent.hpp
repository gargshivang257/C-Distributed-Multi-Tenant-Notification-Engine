#ifndef ROUTING_AGENT_HPP
#define ROUTING_AGENT_HPP

#include <string>
#include <iostream>
#include <algorithm>
#include "gateway/NotificationPayload.hpp"

class RoutingAgent {
public:
    static RoutingAgent& Instance() {
        static RoutingAgent instance;
        return instance;
    }

    
    std::string DetermineOptimalRoute(const NotificationPayload& payload, double current_sms_latency) {
        // Rule 1: Intent Analysis - Prioritize high-value transactional tokens [Certain]
        std::string body_upper = payload.body;
        std::transform(body_upper.begin(), body_upper.end(), body_upper.begin(), ::toupper);

        if (body_upper.find("OTP") != std::string::npos || 
            body_upper.find("VERIFICATION") != std::string::npos || 
            body_upper.find("URGENT") != std::string::npos) {
            
            
            if (current_sms_latency > 0.100) { 
                return "WHATSAPP_SECURE";
            }
            return "SMS_PRIORITY";
        }

        
        if (body_upper.find("SPAM") != std::string::npos || 
            body_upper.find("CONTENT CHUNK") != std::string::npos) {
            return "EMAIL_BULK";
        }

       
        return payload.channel; 
    }

private:
    RoutingAgent() = default;
};

#endif 