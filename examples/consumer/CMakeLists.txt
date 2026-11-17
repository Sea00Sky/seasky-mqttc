cmake_minimum_required(VERSION 3.20)

project(mqttc_consumer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 这就是"下游消费者"的样子：find_package 找到装出来的 mqttc，
# 链 mqttc::mqttc 就够，编译选项和依赖（Threads / OpenSSL）由包自己带出来。
find_package(mqttc CONFIG REQUIRED)

add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE mqttc::mqttc)
