#include "providers/NotificationProvider.hpp"
#include <iostream>
#include <atomic>

class MockProvider : public NotificationProvider {
public:
    MockProvider(bool simulate_network_failure = false) 
        : fail_mode_(simulate_network_failure) {}
        
    ~MockProvider() override = default;

    bool Send(const NotificationPayload& payload) override {
        
        total_attempts_processed_.fetch_add(1, std::memory_order_relaxed);

        if (fail_mode_) {
            std::cerr << "[MOCK PROVIDER] Simulating transient downstream network 5XX failure for message: " 
                      << payload.message_id << "\n";
            return false;
        }

        std::cout << "[MOCK PROVIDER] Successfully delivered payload over simulated network. Channel: " 
                  << payload.channel << " | Recipient: " << payload.recipient << "\n";
        return true;
    }

    uint64_t GetProcessedCount() const {
        return total_attempts_processed_.load(std::memory_order_relaxed);
    }

private:
    bool fail_mode_;
    std::atomic<uint64_t> total_attempts_processed_{0};
};