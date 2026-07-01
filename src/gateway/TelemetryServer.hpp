#ifndef TELEMETRY_SERVER_HPP
#define TELEMETRY_SERVER_HPP

#include <string>
#include <thread>
#include <atomic>
#include <iostream>
#include <sstream>

#ifdef _WIN32
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN 
    #endif
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #pragma comment(lib, "Ws2_32.lib")
#else
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <unistd.h>
#endif

#include "gateway/MetricsEngine.hpp"

class TelemetryServer {
public:
    static TelemetryServer& Instance() {
        static TelemetryServer instance;
        return instance;
    }

    void Start(uint16_t port = 8080) {
        if (running_.exchange(true)) return;

#ifdef _WIN32
        WSADATA wsaData;
        if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
            std::cerr << "[TELEMETRY METRICS] Failed to initialize Winsock.\n";
            running_.store(false);
            return;
        }
#endif

        server_fd_ = static_cast<int>(socket(AF_INET, SOCK_STREAM, 0));
        if (server_fd_ == -1) {
            std::cerr << "[TELEMETRY METRICS] Socket allocation error.\n";
            running_.store(false);
            return;
        }

        int opt = 1;
        setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(port);

        if (bind(server_fd_, (struct sockaddr*)&address, sizeof(address)) < 0) {
            std::cerr << "[TELEMETRY METRICS] Socket binding failed on port " << port << ".\n";
#ifdef _WIN32
            closesocket(server_fd_);
#else
            close(server_fd_);
#endif
            running_.store(false);
            return;
        }

        if (listen(server_fd_, 10) < 0) {
            std::cerr << "[TELEMETRY METRICS] Listen state failed.\n";
            running_.store(false);
            return;
        }

        std::cout << "[TELEMETRY METRICS] Asynchronous HTTP Telemetry Server live on http://localhost:" << port << "/metrics\n";
        
        server_thread_ = std::thread(&TelemetryServer::ListenLoop, this);
    }

    void Stop() {
        if (!running_.exchange(false)) return;

#ifdef _WIN32
        closesocket(server_fd_);
        WSACleanup();
#else
        close(server_fd_);
#endif

        if (server_thread_.joinable()) {
            server_thread_.join();
        }
        std::cout << "[TELEMETRY METRICS] Telemetry Server shutdown complete.\n";
    }

private:
    TelemetryServer() = default;
    ~TelemetryServer() { Stop(); }

    void ListenLoop() {
        while (running_.load()) {
            sockaddr_in client_addr{};
            int addr_len = sizeof(client_addr);
#ifdef _WIN32
            int client_fd = static_cast<int>(accept(server_fd_, (struct sockaddr*)&client_addr, &addr_len));
#else
            int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, (socklen_t*)&addr_len);
#endif

            if (client_fd < 0) continue;

            
            char buffer[1024] = {0};
#ifdef _WIN32
            recv(client_fd, buffer, sizeof(buffer) - 1, 0);
#else
            read(client_fd, buffer, sizeof(buffer) - 1);
#endif

            
            std::string metric_payload = MetricsEngine::Instance().GeneratePrometheusFormat();

            std::stringstream http_response;
            http_response << "HTTP/1.1 200 OK\r\n"
                          << "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
                          << "Content-Length: " << metric_payload.length() << "\r\n"
                          << "Connection: close\r\n\r\n"
                          << metric_payload;

            std::string response_str = http_response.str();
            send(client_fd, response_str.c_str(), static_cast<int>(response_str.length()), 0);

#ifdef _WIN32
            closesocket(client_fd);
#else
            close(client_fd);
#endif
        }
    }

    std::atomic<bool> running_{false};
    int server_fd_{-1};
    std::thread server_thread_;
};

#endif 