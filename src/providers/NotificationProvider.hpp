#ifndef NOTIFICATION_PROVIDER_HPP
#define NOTIFICATION_PROVIDER_HPP

#include "gateway/NotificationPayload.hpp"

class NotificationProvider {
public:
    virtual ~NotificationProvider() = default;

    
    virtual bool Send(const NotificationPayload& payload) = 0;
};

#endif 