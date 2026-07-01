#include "worker/WorkerPool.hpp"
#include <iostream>
#include <chrono>
#include <mutex>
#include <sstream>
#include <cmath>
#include "gateway/Logger.hpp"
#include "queue/RabbitClient.hpp" 
#include "agent/RoutingAgent.hpp" 

WorkerPool::WorkerPool(size_t total_threads, RabbitClient& rabbit_client)
    : total_threads_(total_threads), client_(rabbit_client) {
    
    critical_count_ = static_cast<size_t>(static_cast<double>(total_threads_) * 0.70);
    if (critical_count_ == 0) critical_count_ = 1;
    dynamic_count_ = total_threads_ - critical_count_;

    local_caches_.reserve(total_threads_);
    for (size_t i = 0; i < total_threads_; ++i) {
        
        local_caches_.push_back(std::make_unique<LockFreeDeque>(1024));
    }
}

WorkerPool::~WorkerPool() {
    Stop();
}

void WorkerPool::Start() {
    running_.store(true);
    workers_.reserve(total_threads_);

    size_t worker_id = 0;

    for (size_t i = 0; i < critical_count_; ++i) {
        workers_.emplace_back([this, worker_id](std::stop_token st) {
            CriticalWorkerLoop(st, worker_id);
        });
        worker_id++;
    }

    for (size_t i = 0; i < dynamic_count_; ++i) {
        workers_.emplace_back([this, worker_id](std::stop_token st) {
            DynamicWorkerLoop(st, worker_id);
        });
        worker_id++;
    }

    std::cout << "[WORKER POOL] Initialized Lock-Free Atomic Work-Stealing Pool. Critical: " << critical_count_
              << " | Dynamic Stealing: " << dynamic_count_ << "\n";
}

void WorkerPool::Stop() {
    if (running_.exchange(false)) {
        std::cout << "[WORKER POOL] Signaling all execution loops for defensive teardown...\n";
        cv_.notify_all();
        workers_.clear();
    }
}

void WorkerPool::ProcessPayload(const NotificationPayload& payload, size_t worker_id) {
    double mock_current_sms_latency = 0.142;

    std::string dynamic_route = RoutingAgent::Instance().DetermineOptimalRoute(payload, mock_current_sms_latency);

    std::stringstream log_builder;
    log_builder << "[WORKER #" << worker_id << "][" << payload.tenant_id 
                << "] Routed message: " << payload.message_id 
                << " via Optimized Channel: -> [" << dynamic_route << "]";

    SafeLogger::Instance().LogLine(log_builder.str());
}

void WorkerPool::CriticalWorkerLoop(std::stop_token stop_token, size_t worker_id) {
    while (!stop_token.stop_requested()) {
        
        std::optional<NotificationPayload> payload = local_caches_[worker_id]->PopBack();

        if (!payload) {
            auto network_payload = client_.ConsumeMessage("critical-priority");
            if (network_payload) {
                payload = *network_payload;
                payload->enqueued_at = std::chrono::steady_clock::now();
            }
        }

        if (payload) {
            ProcessPayload(*payload, worker_id);
        } else {
            std::unique_lock<std::mutex> lock(pool_mutex_);
            cv_.wait_for(lock, stop_token, std::chrono::milliseconds(5), [this]() {
                return !running_.load();
            });
        }
    }
}

void WorkerPool::DynamicWorkerLoop(std::stop_token stop_token, size_t worker_id) {
    while (!stop_token.stop_requested()) {
        std::optional<NotificationPayload> payload = local_caches_[worker_id]->PopBack(); 

        
        if (!payload) {
            auto now = std::chrono::steady_clock::now();
            bool stole_any = false; 
            
            for (size_t victim_id = 0; victim_id < critical_count_; ++victim_id) {
                std::optional<NotificationPayload> oldest_msg = local_caches_[victim_id]->PeekFront();
                
                if (oldest_msg) {
                    auto delta_t = std::chrono::duration_cast<std::chrono::milliseconds>(now - oldest_msg->enqueued_at).count();
                    double aged_priority = 1.0 + 10.0 * (1.0 - std::exp(-0.005 * static_cast<double>(delta_t)));

                    if (aged_priority > 1.05) { 
                        payload = local_caches_[victim_id]->StealFront(); 
                        
                        if (payload) { 
                            std::stringstream steal_log;
                            steal_log << "[ATOMIC LOCK-FREE SCHEDULER] Dynamic Thread #" << worker_id 
                                      << " STOLE aged message " << payload->message_id 
                                      << " (Score: " << aged_priority 
                                      << " | Latency: " << delta_t << "ms) lock-free from Thread #" << victim_id;
                            SafeLogger::Instance().LogLine(steal_log.str());
                            stole_any = true;
                            break;
                        }
                    }
                }
            }

            
            if (!stole_any) {
                payload = std::nullopt;
            }
        }

        if (!payload) {
            auto network_payload = client_.ConsumeMessage("default-priority");
            if (network_payload) {
                payload = *network_payload;
                payload->enqueued_at = std::chrono::steady_clock::now();
            }
        }

        if (payload) {
            ProcessPayload(*payload, worker_id);
        } else {
            
            std::unique_lock<std::mutex> lock(pool_mutex_);
            cv_.wait_for(lock, stop_token, std::chrono::milliseconds(10), [this]() {
                return !running_.load();
            });
        }
    }
}