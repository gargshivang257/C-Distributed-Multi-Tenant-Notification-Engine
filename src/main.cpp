#include "gateway/IdempotencyEngine.hpp"
#include "gateway/ShardedRateLimiter.hpp"
#include "gateway/MetricsEngine.hpp"
#include "queue/RabbitClient.hpp"
#include "worker/WorkerPool.hpp"
#include "worker/RetryJitter.hpp"
#include "worker/DlqSanitizer.hpp"
#include "gateway/Logger.hpp"
#include "gateway/TelemetryServer.hpp"

#include <iostream>
#include <vector>
#include <thread>
#include <chrono>

int main() {
    SafeLogger::Instance().LogLine("=== STARTING MULTI-TENANT NOTIFICATION ENGINE CORE RUNTIME ===\n");

    
    std::chrono::seconds cache_ttl(60);
    IdempotencyEngine idempotency_engine(cache_ttl, "127.0.0.1", 6379);
    
    
    ShardedRateLimiter rate_limiter(100.0, 0.25);
    
    RabbitClient rabbit_client("amqp://localhost:5672");
    
   
    WorkerPool worker_pool(10, rabbit_client);
    worker_pool.Start();

    
    TelemetryServer::Instance().Start(8080);

    
    SafeLogger::Instance().LogLine("\n[SIMULATION] Spawning high-contention traffic threads hitting the gateway...");
    std::vector<std::thread> traffic_threads;
    
    for (int t = 0; t < 4; ++t) {
        traffic_threads.emplace_back([&idempotency_engine, &rate_limiter, &rabbit_client, &worker_pool, t]() {
            std::string tenant_id = (t % 2 == 0) ? "tenant-alpha" : "tenant-beta";
            
            for (int i = 0; i < 20; ++i) {
                std::string raw_body;
                
                if (i % 3 == 0) {
                    raw_body = "Your OTP secure verification login token is 4921";
                } else if (i % 3 == 1) {
                    raw_body = "URGENT Account Security Breach Notice Action Required";
                } else {
                    raw_body = "Standard promotional marketing bulk spam content chunk";
                }
                
                std::string hash_key = idempotency_engine.ComputeSha256(raw_body);
                bool is_unique = idempotency_engine.TryInsert(hash_key, tenant_id);
                
                if (!is_unique) {
                    MetricsEngine::Instance().IncrementIngestionCount(tenant_id, 409);
                    continue;
                }

                if (!rate_limiter.AllowRequest(tenant_id)) {
                    MetricsEngine::Instance().IncrementIngestionCount(tenant_id, 429);
                    idempotency_engine.UpdateStatus("idempotency:" + tenant_id + ":" + hash_key, "RATE_LIMITED_REJECTED");
                    continue;
                }

                MetricsEngine::Instance().IncrementIngestionCount(tenant_id, 200);

                NotificationPayload payload{
                    "msg-uuid-" + std::to_string(t) + "-" + std::to_string(i),
                    tenant_id,
                    "recipient@domain.com",
                    "SMS",
                    raw_body,
                    0,
                    std::chrono::steady_clock::now()
                };
                
                std::string queue_target = (i % 4 == 0) ? "critical-priority" : "default-priority";
                rabbit_client.PublishMessage(queue_target, payload);
                
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        });
    }

    
    for (auto& th : traffic_threads) {
        if (th.joinable()) th.join();
    }

    
    SafeLogger::Instance().LogLine("\n[SIMULATION] Injecting high-contention memory burst to test mathematical work-stealing...");
    for (int i = 0; i < 50; ++i) {
        std::string test_body;
        if (i % 2 == 0) {
            test_body = "URGENT Priority Security Passcode Payload Change Request";
        } else {
            test_body = "Bulk system logging tracking record spam content chunk";
        }

        NotificationPayload test_payload{
            "steal-test-id-" + std::to_string(i),
            "tenant-alpha",
            "admin@domain.com",
            "SMS",
            test_body,
            0,
            
            std::chrono::steady_clock::now() - std::chrono::milliseconds(200)
        };

        worker_pool.InjectDirectTestPayload(test_payload, true); 
    }

   
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    
    SafeLogger::Instance().LogLine("\n[FAULT TOLERANCE TEST] Simulating a message failure to check DLQ serialization...");
    NotificationPayload failing_payload{"err-uuid-999", "tenant-gamma", "admin@domain.com", "EMAIL", "Crash test data", 5, std::chrono::steady_clock::now()};
    std::string serialized_dlq = DlqSanitizer::SerializeSanitized(failing_payload, "INVALID_PAYLOAD");
    SafeLogger::Instance().LogLine("[DLQ OUTPUT SANITIZED SCHEMA]:\n" + serialized_dlq);

    SafeLogger::Instance().LogLine("\n[FAULT TOLERANCE TEST] Calculating thread-isolated jitter intervals...");
    for (int attempt = 1; attempt <= 3; ++attempt) {
        auto wait_time = RetryJitter::CalculateWait(attempt, 100.0, 1000.0);
        SafeLogger::Instance().LogLine("   Attempt #" + std::to_string(attempt) + 
                                       " Jittered Backoff Window calculated: " + 
                                       std::to_string(wait_time.count()) + "ms");
    }

    
    MetricsEngine::Instance().UpdateQueueDepth(0);
    MetricsEngine::Instance().RecordDeliveryLatency(0.042);
    MetricsEngine::Instance().DumpMetricsReport();

    
    SafeLogger::Instance().LogLine("\n[PROMETHEUS TELEMETRY] HTTP Exporter live on http://localhost:8080/metrics");
    SafeLogger::Instance().LogLine("[PROMETHEUS TELEMETRY] Press ENTER in this console to flush metrics and terminate runtime...");
    std::cin.get(); 

    
    SafeLogger::Instance().LogLine("\n[TEARDOWN] Flushing memory pools and tearing down network interfaces...");
    worker_pool.Stop();
    TelemetryServer::Instance().Stop();

    SafeLogger::Instance().LogLine("=== CORE ENGINE RUNTIME TEST EXECUTION SUCCESSFUL ===");
    return 0;
}