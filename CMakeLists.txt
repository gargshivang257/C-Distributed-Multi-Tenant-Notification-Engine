cmake_minimum_required(VERSION 3.20)
project(MultiTenantNotificationEngine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|MSVC")
    add_compile_options(/W4 /O2)
endif()

# Find packages provided via vcpkg registry mapping
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(hiredis REQUIRED)
find_package(amqpcpp REQUIRED) 

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

# Define the static engine core compilation library target
add_library(gateway_lib STATIC
    src/gateway/IdempotencyEngine.cpp
    src/gateway/ShardedRateLimiter.cpp
    src/queue/RabbitClient.cpp
    src/worker/WorkerPool.cpp
    src/providers/MockProvider.cpp
    src/gateway/MetricsEngine.cpp
)
# Link using verified target name strings [Certain]
target_link_libraries(gateway_lib PRIVATE 
    OpenSSL::Crypto 
    Threads::Threads
    hiredis::hiredis
    amqpcpp         # <-- FIXED: Removed invalid namespace prefix
)

# Define the runnable application compilation target
add_executable(notification_engine
    src/main.cpp
)
target_link_libraries(notification_engine PRIVATE 
    gateway_lib 
    OpenSSL::Crypto 
    Threads::Threads
    hiredis::hiredis
    amqpcpp         # <-- FIXED: Removed invalid namespace prefix
)