#ifndef LOGGER_HPP
#define LOGGER_HPP

#include <iostream>
#include <string>
#include <mutex>

class SafeLogger {
public:
    static SafeLogger& Instance() {
        static SafeLogger instance;
        return instance;
    }

    
    void LogLine(const std::string& accurate_log_line) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << accurate_log_line << std::endl;
    }

private:
    SafeLogger() = default;
    std::mutex mutex_;
};

#endif 