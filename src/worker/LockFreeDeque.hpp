#ifndef LOCK_FREE_DEQUE_HPP
#define LOCK_FREE_DEQUE_HPP

#include <atomic>
#include <optional>
#include <vector>
#include "gateway/NotificationPayload.hpp"

class LockFreeDeque {
public:
    LockFreeDeque(size_t capacity = 1024) 
        : capacity_(capacity), ring_buffer_(capacity), head_(0), tail_(0) {}

    
    bool PushBack(const NotificationPayload& payload) {
        size_t t = tail_.load(std::memory_order_relaxed);
        size_t h = head_.load(std::memory_order_acquire);

        if ((t - h) >= capacity_) {
            return false; 
        }

        ring_buffer_[t % capacity_] = payload;
        
        tail_.store(t + 1, std::memory_order_release);
        return true;
    }

    
    std::optional<NotificationPayload> PopBack() {
        size_t t = tail_.load(std::memory_order_relaxed);
        if (t == 0) return std::nullopt; 

        t--;
        tail_.store(t, std::memory_order_seq_cst); 

        size_t h = head_.load(std::memory_order_seq_cst);
        if (h <= t) {
            NotificationPayload payload = ring_buffer_[t % capacity_];
            if (h == t) {
                
                if (!head_.compare_exchange_strong(h, h + 1, std::memory_order_seq_cst, std::memory_order_relaxed)) {
                    
                    tail_.store(t + 1, std::memory_order_relaxed);
                    return std::nullopt;
                }
                tail_.store(0, std::memory_order_relaxed);
                head_.store(0, std::memory_order_relaxed);
            }
            return payload;
        } else {
            tail_.store(t + 1, std::memory_order_relaxed);
            return std::nullopt;
        }
    }

    
    std::optional<NotificationPayload> StealFront() {
        while (true) {
            size_t h = head_.load(std::memory_order_seq_cst);
            size_t t = tail_.load(std::memory_order_seq_cst);

            if (h >= t) {
                return std::nullopt; 
            }

            NotificationPayload payload = ring_buffer_[h % capacity_];
           
            if (head_.compare_exchange_strong(h, h + 1, std::memory_order_seq_cst, std::memory_order_relaxed)) {
                return payload;
            }
        }
    }

    bool Empty() const {
        return head_.load(std::memory_order_relaxed) >= tail_.load(std::memory_order_relaxed);
    }

    
    std::optional<NotificationPayload> PeekFront() {
        size_t h = head_.load(std::memory_order_acquire);
        size_t t = tail_.load(std::memory_order_acquire);
        if (h >= t) return std::nullopt;
        return ring_buffer_[h % capacity_];
    }

private:
    size_t capacity_;
    std::vector<NotificationPayload> ring_buffer_;
    std::atomic<size_t> head_;
    std::atomic<size_t> tail_;
};

#endif 