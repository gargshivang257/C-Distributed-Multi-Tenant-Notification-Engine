#ifndef METRICS_ENGINE_HPP
#define METRICS_ENGINE_HPP

#include <string>
#include <atomic>
#include <array>
#include <iostream>
#include <sstream> 

class MetricsEngine {
public:
    static MetricsEngine& Instance() {
        static MetricsEngine instance;
        return instance;
    }

    
    void IncrementIngestionCount(const std::string& tenant_id, int status_code) {
        size_t tenant_idx = GetTenantIndex(tenant_id);
        
        if (status_code >= 200 && status_code < 300) {
            ingestion_success_counts_[tenant_idx].fetch_add(1, std::memory_order_relaxed);
        } else if (status_code == 409) {
            ingestion_duplicate_counts_[tenant_idx].fetch_add(1, std::memory_order_relaxed);
        } else if (status_code == 429) {
            ingestion_ratelimit_counts_[tenant_idx].fetch_add(1, std::memory_order_relaxed);
        } else {
            ingestion_failure_counts_[tenant_idx].fetch_add(1, std::memory_order_relaxed);
        }
    }

    void UpdateQueueDepth(int64_t current_depth) {
        queue_depth_current_.store(current_depth, std::memory_order_relaxed);
    }

    void RecordDeliveryLatency(double seconds) {
        
        double current_total = total_latency_accumulated_.load(std::memory_order_relaxed);
        total_latency_accumulated_.store(current_total + seconds, std::memory_order_relaxed);
        latency_sample_count_.fetch_add(1, std::memory_order_relaxed);
    }

    
    void DumpMetricsReport() {
        std::cout << "\n=== PRODUCTION METRICS MATRIX SYSTEM REPORT ===\n";
        std::cout << "queue_depth_current: " << queue_depth_current_.load() << "\n";
        
        for (size_t i = 0; i < ALLOWED_TENANTS.size(); ++i) {
            std::cout << "notification_ingestion_rate_total{tenant=\"" << ALLOWED_TENANTS[i] 
                      << "\", status=\"2xx\"}: " << ingestion_success_counts_[i].load() << "\n";
            std::cout << "notification_ingestion_rate_total{tenant=\"" << ALLOWED_TENANTS[i] 
                      << "\", status=\"409\"}: " << ingestion_duplicate_counts_[i].load() << "\n";
            std::cout << "notification_ingestion_rate_total{tenant=\"" << ALLOWED_TENANTS[i] 
                      << "\", status=\"429\"}: " << ingestion_ratelimit_counts_[i].load() << "\n";
            std::cout << "notification_ingestion_rate_total{tenant=\"" << ALLOWED_TENANTS[i] 
                      << "\", status=\"5xx\"}: " << ingestion_failure_counts_[i].load() << "\n";
        }
        
        uint64_t samples = latency_sample_count_.load();
        double avg_latency = (samples > 0) ? (total_latency_accumulated_.load() / static_cast<double>(samples)) : 0.0;
        std::cout << "notification_delivery_latency_seconds (average): " << avg_latency << "s\n";
        std::cout << "================================================\n\n";
    }

    
    std::string GeneratePrometheusFormat() {
        std::stringstream ss;
        
        ss << "# HELP queue_depth_current Current number of backlogged messages in engine storage.\n"
           << "# TYPE queue_depth_current gauge\n"
           << "queue_depth_current " << queue_depth_current_.load(std::memory_order_relaxed) << "\n\n";

        uint64_t samples = latency_sample_count_.load(std::memory_order_relaxed);
        double avg_latency = (samples > 0) ? (total_latency_accumulated_.load(std::memory_order_relaxed) / static_cast<double>(samples)) : 0.0;
        
        ss << "# HELP notification_delivery_latency_seconds Average system delivery time metrics.\n"
           << "# TYPE notification_delivery_latency_seconds gauge\n"
           << "notification_delivery_latency_seconds " << avg_latency << "\n\n";

        ss << "# HELP notification_ingestion_rate_total Cumulative count of notification events processed.\n"
           << "# TYPE notification_ingestion_rate_total counter\n";

        for (size_t i = 0; i < ALLOWED_TENANTS.size(); ++i) {
        ss << "notification_ingestion_rate_total{tenant=\"" << ALLOWED_TENANTS[i] << "\",status=\"200\"} " 
           << ingestion_success_counts_[i].load(std::memory_order_relaxed) << "\n";
        ss << "notification_ingestion_rate_total{tenant=\"" << ALLOWED_TENANTS[i] << "\",status=\"409\"} " 
           << ingestion_duplicate_counts_[i].load(std::memory_order_relaxed) << "\n";
        ss << "notification_ingestion_rate_total{tenant=\"" << ALLOWED_TENANTS[i] << "\",status=\"429\"} " 
           << ingestion_ratelimit_counts_[i].load(std::memory_order_relaxed) << "\n";
        ss << "notification_ingestion_rate_total{tenant=\"" << ALLOWED_TENANTS[i] << "\",status=\"500\"} " 
           << ingestion_failure_counts_[i].load(std::memory_order_relaxed) << "\n";
    }
           
        return ss.str();
    }

private:
    MetricsEngine() = default;

    static constexpr std::array<const char*, 3> ALLOWED_TENANTS = {"tenant-alpha", "tenant-beta", "tenant-gamma"};
    
    std::array<std::atomic<uint64_t>, 3> ingestion_success_counts_{{0, 0, 0}};
    std::array<std::atomic<uint64_t>, 3> ingestion_duplicate_counts_{{0, 0, 0}};
    std::array<std::atomic<uint64_t>, 3> ingestion_ratelimit_counts_{{0, 0, 0}};
    std::array<std::atomic<uint64_t>, 3> ingestion_failure_counts_{{0, 0, 0}};
    
    std::atomic<int64_t> queue_depth_current_{0};
    std::atomic<double> total_latency_accumulated_{0.0};
    std::atomic<uint64_t> latency_sample_count_{0};

    size_t GetTenantIndex(const std::string& tenant_id) const {
        for (size_t i = 0; i < ALLOWED_TENANTS.size(); ++i) {
            if (tenant_id == ALLOWED_TENANTS[i]) return i;
        }
        return 0; 
    }
};

#endif 