#ifndef WORKER_POOL_HPP
#define WORKER_POOL_HPP

#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <optional>
#include <string>
#include <memory>

#include "gateway/NotificationPayload.hpp"
#include "worker/LockFreeDeque.hpp" 

class RabbitClient;

class WorkerPool {
public:
    WorkerPool(size_t total_threads, RabbitClient& rabbit_client);
    ~WorkerPool();

    void Start();
    void Stop();

    
    void InjectDirectTestPayload(NotificationPayload payload, bool is_critical) {
        size_t target_worker = 0;
        if (is_critical) {
            target_worker = rand() % critical_count_;
        } else {
            target_worker = critical_count_ + (rand() % dynamic_count_);
        }
        
        
        local_caches_[target_worker]->PushBack(payload);
    }

private:
    void CriticalWorkerLoop(std::stop_token stop_token, size_t worker_id);
    void DynamicWorkerLoop(std::stop_token stop_token, size_t worker_id);
    void ProcessPayload(const NotificationPayload& payload, size_t worker_id);

    size_t total_threads_;
    size_t critical_count_;
    size_t dynamic_count_;
    RabbitClient& client_;

    std::atomic<bool> running_{false};
    std::vector<std::jthread> workers_; 
    std::mutex pool_mutex_;
    std::condition_variable_any cv_;

    
    std::vector<std::unique_ptr<LockFreeDeque>> local_caches_;
};

#endif 