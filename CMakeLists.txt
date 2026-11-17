cmake_minimum_required(VERSION 3.20)

project(mqttc
    VERSION 0.2.0
    DESCRIPTION "MQTT 3.1.1 client library (QoS 0/1) and a small CLI"
    LANGUAGES CXX)

# ---------------------------------------------------------------- 选项

option(MQTTC_BUILD_CLI       "构建命令行工具"                    ON)
option(MQTTC_BUILD_TESTS     "构建单元测试"                      ON)
option(MQTTC_BUILD_ASYNC     "构建异步外壳（IO 线程 + 有界队列）" ON)
# TLS 默认开（已锁定的范围）。实现只在 POSIX 上（src/tls_transport.cpp），
# Windows 侧还没写——需要 MSYS2 + OpenSSL 的环境来编写与验证，CMake 在这里明确拦住，
# 而不是编出一个用不了的 TlsTransport。
option(MQTTC_ENABLE_TLS      "启用 TLS（需要 OpenSSL；POSIX）" ON)
option(MQTTC_BUILD_FUZZERS   "构建 libFuzzer 目标（需要 clang）"  OFF)
option(MQTTC_ENABLE_COVERAGE "生成 gcov 覆盖率报告"              OFF)
set(MQTTC_SANITIZE "" CACHE STRING "sanitizer 列表：address,undefined 或 thread（空 = 不启用）")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "" FORCE)
endif()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# CLI 与后续的异步外壳都用到 std::thread；MinGW 上侥幸不报错，Linux 上是硬链接错误
find_package(Threads REQUIRED)

if(MQTTC_ENABLE_TLS)
    if(WIN32)
        message(FATAL_ERROR
            "Windows 侧的 TLS 传输层尚未实现（需要 MSYS2 + OpenSSL 的环境来编写与验证）。"
            "现在请用 -DMQTTC_ENABLE_TLS=OFF。")
    endif()
    find_package(OpenSSL REQUIRED)
    if(NOT OpenSSL_FOUND)
        message(FATAL_ERROR
            "MQTTC_ENABLE_TLS=ON 但没找到 OpenSSL。装一个（Debian/Ubuntu: libssl-dev），"
            "或者显式关闭：-DMQTTC_ENABLE_TLS=OFF")
    endif()
endif()

# 自己代码的编译选项：-Werror 只作用于本项目目标，不会传给下游消费者
function(mqttc_target_defaults target)
    target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic -Werror)
    if(MQTTC_SANITIZE)
        target_compile_options(${target} PRIVATE
            -fsanitize=${MQTTC_SANITIZE} -fno-omit-frame-pointer -g)
        target_link_options(${target} PRIVATE -fsanitize=${MQTTC_SANITIZE})
    endif()
    if(MQTTC_ENABLE_COVERAGE)
        target_compile_options(${target} PRIVATE --coverage -O0 -g)
        target_link_options(${target} PRIVATE --coverage)
    endif()
endfunction()

# ---------------------------------------------------------------- 库

set(MQTTC_SOURCES
    src/wire.cpp
    src/topic.cpp
    src/packet.cpp
    src/client.cpp
    src/failover.cpp
)

# log.cpp 是可观测性阶段落的，文件存在才纳入构建
foreach(extra src/log.cpp)
    if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${extra})
        list(APPEND MQTTC_SOURCES ${extra})
    endif()
endforeach()

if(MQTTC_BUILD_ASYNC)
    if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/src/async_client.cpp)
        message(FATAL_ERROR "MQTTC_BUILD_ASYNC=ON 但 src/async_client.cpp 不存在")
    endif()
    list(APPEND MQTTC_SOURCES src/async_client.cpp)
endif()

add_library(mqttc ${MQTTC_SOURCES})
add_library(mqttc::mqttc ALIAS mqttc)

set_target_properties(mqttc PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
    POSITION_INDEPENDENT_CODE ON)

if(WIN32)
    target_sources(mqttc PRIVATE src/transport_win.cpp)
    target_link_libraries(mqttc PUBLIC ws2_32)
else()
    target_sources(mqttc PRIVATE src/transport_posix.cpp)
endif()

target_link_libraries(mqttc PUBLIC Threads::Threads)

if(MQTTC_BUILD_ASYNC)
    target_compile_definitions(mqttc PUBLIC MQTTC_WITH_ASYNC=1)
else()
    target_compile_definitions(mqttc PUBLIC MQTTC_WITH_ASYNC=0)
endif()

if(MQTTC_ENABLE_TLS)
    target_sources(mqttc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/tls_transport.cpp)
    target_link_libraries(mqttc PUBLIC OpenSSL::SSL OpenSSL::Crypto)
    target_compile_definitions(mqttc PUBLIC MQTTC_WITH_TLS=1)
else()
    target_compile_definitions(mqttc PUBLIC MQTTC_WITH_TLS=0)
endif()

target_include_directories(mqttc PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

mqttc_target_defaults(mqttc)

# ---------------------------------------------------------------- CLI

if(MQTTC_BUILD_CLI)
    add_executable(mqttc_cli tools/mqttc_main.cpp)
    set_target_properties(mqttc_cli PROPERTIES OUTPUT_NAME mqttc)
    target_link_libraries(mqttc_cli PRIVATE mqttc)
    mqttc_target_defaults(mqttc_cli)
endif()

# ---------------------------------------------------------------- 测试

if(MQTTC_BUILD_TESTS)
    enable_testing()

    set(MQTTC_TEST_SOURCES
        tests/test_main.cpp
        tests/test_wire.cpp
        tests/test_topic.cpp
        tests/test_packet.cpp
        tests/test_client.cpp
        tests/test_client_extra.cpp
        tests/test_reconnect.cpp
        tests/test_failover.cpp
        tests/test_async.cpp
        tests/test_fuzz_lite.cpp
        tests/test_log.cpp
        tests/test_transport.cpp
    )

    add_executable(mqttc_tests ${MQTTC_TEST_SOURCES})
    target_include_directories(mqttc_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(mqttc_tests PRIVATE mqttc)
    mqttc_target_defaults(mqttc_tests)

    add_test(NAME mqttc_tests COMMAND mqttc_tests)

    # 端到端：真 socket + 自带的 Python mock broker（有 Python 与 CLI 才挂上）
    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(Python3_Interpreter_FOUND AND MQTTC_BUILD_CLI)
        set(MQTTC_E2E_ARGS
            --binary $<TARGET_FILE:mqttc_cli>
            --work-dir ${CMAKE_CURRENT_BINARY_DIR}/e2e)
        # TLS 构建才跑 TLS 场景（证书由脚本用 openssl CLI 在构建目录里现生成）
        if(MQTTC_ENABLE_TLS)
            list(APPEND MQTTC_E2E_ARGS --tls)
        endif()
        add_test(NAME mqttc_e2e
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/e2e/run_e2e.py
                    ${MQTTC_E2E_ARGS})
        set_tests_properties(mqttc_e2e PROPERTIES TIMEOUT 180)
    endif()
endif()

# ---------------------------------------------------------------- 模糊测试

# libFuzzer 随 clang 提供，运行时在 libclang-rt-<版本>-dev 包里。
# Linux（clang 18 + libclang-rt-18-dev）上已实际构建并运行过，见 README"验证状态"；
# Windows/MinGW 没有这条工具链，所以这里只对 clang 放开。
if(MQTTC_BUILD_FUZZERS)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR
            "MQTTC_BUILD_FUZZERS=ON 需要 clang（libFuzzer 由 clang 提供），"
            "当前编译器是 ${CMAKE_CXX_COMPILER_ID}")
    endif()

    foreach(fuzzer fuzz_decode fuzz_wire)
        add_executable(${fuzzer} fuzz/${fuzzer}.cc)
        target_link_libraries(${fuzzer} PRIVATE mqttc)
        target_compile_options(${fuzzer} PRIVATE
            -Wall -Wextra -Wpedantic -Werror -g -fno-omit-frame-pointer
            -fsanitize=fuzzer,address,undefined)
        target_link_options(${fuzzer} PRIVATE -fsanitize=fuzzer,address,undefined)
    endforeach()
endif()

# ---------------------------------------------------------------- 安装与导出

install(TARGETS mqttc
    EXPORT mqttcTargets
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

if(MQTTC_BUILD_CLI)
    install(TARGETS mqttc_cli RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()

install(DIRECTORY include/mqttc DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT mqttcTargets
    NAMESPACE mqttc::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/mqttc)

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/mqttcConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/mqttcConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/mqttc)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/mqttcConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/mqttcConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/mqttcConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/mqttc)
