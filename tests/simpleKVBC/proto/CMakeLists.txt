find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

include_directories(${GRPC_INCLUDE_DIR})

protobuf_generate_cpp(ADAPTIVE_TIMERS_PROTO_SRCS ADAPTIVE_TIMERS_PROTO_HDRS adaptive_timers.proto)
grpc_generate_cpp(ADAPTIVE_TIMERS_GRPC_SRCS ADAPTIVE_TIMERS_GRPC_HDRS ${CMAKE_CURRENT_BINARY_DIR}
  adaptive_timers.proto
)
message(STATUS "Adaptive timers gRPC/protobuf generated - see " ${CMAKE_CURRENT_BINARY_DIR})

add_library(adaptive-timers-proto STATIC ${ADAPTIVE_TIMERS_PROTO_SRCS} ${ADAPTIVE_TIMERS_PROTO_HDRS}
            ${ADAPTIVE_TIMERS_GRPC_SRCS} ${ADAPTIVE_TIMERS_GRPC_HDRS})
target_link_libraries(adaptive-timers-proto protobuf::libprotobuf gRPC::grpc++)
target_include_directories(adaptive-timers-proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
