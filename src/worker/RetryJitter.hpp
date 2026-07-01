#ifndef RETRY_JITTER_HPP
#define RETRY_JITTER_HPP

#include <random>
#include <algorithm>
#include <chrono>
#include <thread>

class RetryJitter {
public:
   
    static std::chrono::milliseconds CalculateWait(int attempt, double base_ms, double max_backoff_ms) {
        
        thread_local std::mt19937 generator(static_cast<unsigned int>(
            std::chrono::system_clock::now().time_since_epoch().count() + 
            std::hash<std::thread::id>{}(std::this_thread::get_id()))
        );

       
        double exponential_factor = base_ms * static_cast<double>(1LL << std::min(attempt, 30));
        double upper_bound = std::min(max_backoff_ms, exponential_factor);

        
        std::uniform_real_distribution<double> distribution(0.0, upper_bound);
        return std::chrono::milliseconds(static_cast<long long>(distribution(generator)));
    }
};

#endif 