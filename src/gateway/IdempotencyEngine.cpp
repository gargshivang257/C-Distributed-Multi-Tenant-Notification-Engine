#include "gateway/IdempotencyEngine.hpp"
#include "gateway/Logger.hpp"
#include <openssl/evp.h>
#include <iomanip>
#include <sstream>

IdempotencyEngine::IdempotencyEngine(std::chrono::seconds entry_ttl, const std::string& redis_host, int redis_port)
    : redis_ctx_(nullptr), ttl_(entry_ttl) {
    
    
    redis_ctx_ = redisConnect(redis_host.c_str(), redis_port);
    
    std::stringstream log_builder;
    if (redis_ctx_ == nullptr || redis_ctx_->err) {
        if (redis_ctx_) {
            log_builder << "[REDIS KERNEL] Initial connection failed: " << redis_ctx_->errstr;
            redisFree(redis_ctx_);
            redis_ctx_ = nullptr;
        } else {
            log_builder << "[REDIS KERNEL] Context allocation out-of-memory structural error.";
        }
    } else {
        log_builder << "[REDIS KERNEL] Core driver mapped. Connected to host cluster at " << redis_host << ":" << redis_port;
    }
    SafeLogger::Instance().LogLine(log_builder.str());
}

IdempotencyEngine::~IdempotencyEngine() {
    std::lock_guard<std::mutex> lock(engine_mutex_);
    if (redis_ctx_ != nullptr) {
        redisFree(redis_ctx_);
    }
}

std::string IdempotencyEngine::ComputeSha256(const std::string& input) const {
    EVP_MD_CTX* context = EVP_MD_CTX_new();
    if (!context) return "";

    if (EVP_DigestInit_ex(context, EVP_sha256(), nullptr) != 1 ||
        EVP_DigestUpdate(context, input.c_str(), input.length()) != 1) {
        EVP_MD_CTX_free(context);
        return "";
    }

    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int length = 0;

    if (EVP_DigestFinal_ex(context, hash, &length) != 1) {
        EVP_MD_CTX_free(context);
        return "";
    }

    EVP_MD_CTX_free(context);

    std::stringstream hex_stream;
    for (unsigned int i = 0; i < length; ++i) {
        hex_stream << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    return hex_stream.str();
}

bool IdempotencyEngine::TryInsert(const std::string& key, const std::string& tenant_id) {
    std::lock_guard<std::mutex> lock(engine_mutex_);
    if (redis_ctx_ == nullptr) return true; 

    
    std::string composite_key = "idempotency:" + tenant_id + ":" + key;
    
    
    redisReply* reply = (redisReply*)redisCommand(redis_ctx_, "SETNX %s PENDING", composite_key.c_str());
    if (reply == nullptr) {
        SafeLogger::Instance().LogLine("[REDIS KERNEL] Command error tracking state instance context.");
        return true;
    }

    long long status_code = reply->integer;
    freeReplyObject(reply);

    if (status_code == 1) {
        
        redisReply* expire_reply = (redisReply*)redisCommand(redis_ctx_, "EXPIRE %s %lld", 
                                                             composite_key.c_str(), 
                                                             static_cast<long long>(ttl_.count()));
        if (expire_reply != nullptr) {
            freeReplyObject(expire_reply);
        }
        return true; 
    }

    return false; 
}

std::optional<std::string> IdempotencyEngine::GetCachedResponse(const std::string& key) {
    
    (void)key;
    return std::nullopt;
}

void IdempotencyEngine::UpdateStatus(const std::string& key, const std::string& response_body) {
    std::lock_guard<std::mutex> lock(engine_mutex_);
    if (redis_ctx_ == nullptr) return;

    // Updates state data directly inside the global remote memory footprint
    redisReply* reply = (redisReply*)redisCommand(redis_ctx_, "SET %s %s KEEPTTL", key.c_str(), response_body.c_str());
    if (reply != nullptr) {
        freeReplyObject(reply);
    }
}