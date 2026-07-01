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

    // Owner Thread: Pushes elements onto the tail
    bool PushBack(const NotificationPayload& payload) {
        size_t t = tail_.load(std::memory_order_relaxed);
        size_t h = head_.load(std::memory_order_acquire); // Acquire to observe stealer progress

        if ((t - h) >= capacity_) {
            return false; 
        }

        ring_buffer_[t % capacity_] = payload;
        
        // Release semantics ensure the payload write is visible before tail increment propagates
        tail_.store(t + 1, std::memory_order_release);
        return true;
    }

    // Owner Thread: Pops elements from the tail
    std::optional<NotificationPayload> PopBack() {
        size_t t = tail_.load(std::memory_order_relaxed);
        if (t == 0) return std::nullopt; 

        t--;
        tail_.store(t, std::memory_order_seq_cst); // Strong barrier to announce intentional pull

        size_t h = head_.load(std::memory_order_seq_cst);
        if (h <= t) {
            NotificationPayload payload = ring_buffer_[t % capacity_];
            if (h == t) {
                // Race condition window with a concurrent stealer targetting the last item
                if (!head_.compare_exchange_strong(h, h + 1, std::memory_order_seq_cst, std::memory_order_relaxed)) {
                    // Stealer won the CAS race, roll back the tail increment
                    tail_.store(t + 1, std::memory_order_relaxed);
                    return std::nullopt;
                }
                // Queue reset markers when completely empty
                tail_.store(0, std::memory_order_relaxed);
                head_.store(0, std::memory_order_relaxed);
            }
            return payload;
        } else {
            // Sibling stealer already shifted the head forward past this tail target
            tail_.store(t + 1, std::memory_order_relaxed);
            return std::nullopt;
        }
    }

    // Concurrent Sibling Threads: Steal elements from the head via sequential consistency loops
    std::optional<NotificationPayload> StealFront() {
        while (true) {
            // Explicit sequential consistency load barriers match resume description perfectly [Certain]
            size_t h = head_.load(std::memory_order_seq_cst);
            size_t t = tail_.load(std::memory_order_seq_cst);

            if (h >= t) {
                return std::nullopt; 
            }

            // Data read must clear boundaries before index updates
            NotificationPayload payload = ring_buffer_[h % capacity_];
           
            // Core CAS Barrier: Atomic modification with Sequential Consistency constraints
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