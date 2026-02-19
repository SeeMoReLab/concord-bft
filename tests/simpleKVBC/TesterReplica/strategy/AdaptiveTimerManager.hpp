#pragma once

#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include <grpcpp/grpcpp.h>
#include "adaptive_timers.grpc.pb.h"

#include "log/logger.hpp"

namespace concord::kvbc::strategy {

struct MetricsWindow {
  float total_latency_ms{0};
  float max_latency_ms{0};
  float min_latency_ms{std::numeric_limits<float>::max()};
  uint32_t count{0};
  uint32_t slow_path_count{0};
  uint32_t fast_path_count{0};
  std::chrono::steady_clock::time_point start_time{std::chrono::steady_clock::now()};

  void reset() {
    total_latency_ms = 0;
    max_latency_ms = 0;
    min_latency_ms = std::numeric_limits<float>::max();
    count = 0;
    slow_path_count = 0;
    fast_path_count = 0;
    start_time = std::chrono::steady_clock::now();
  }

  float avgLatency() const { return count > 0 ? total_latency_ms / count : 0; }

  float elapsedMs() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start_time)
        .count();
  }
};

class AdaptiveTimerManager {
 public:
  static AdaptiveTimerManager& instance();

  void init(const std::string& agent_addr, uint64_t iteration_count);
  bool isActive() const;

  // Called per consensus round (per PrePrepare seqNum).
  // Returns new slow path timeout in ms if one should be applied now, nullopt otherwise.
  std::optional<uint32_t> onTransaction(uint64_t seqNum);

  // Feed per-transaction metrics
  void recordMetrics(uint32_t slow_count_delta, uint32_t fast_count_delta, float latency_ms);

 private:
  AdaptiveTimerManager() = default;
  AdaptiveTimerManager(const AdaptiveTimerManager&) = delete;
  AdaptiveTimerManager& operator=(const AdaptiveTimerManager&) = delete;

  void sendPredict();
  void sendLearn(const std::string& prediction_id, float reward);
  bool pollPrediction();

  // gRPC
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<LearningAgent::Stub> stub_;

  // Config
  uint64_t iteration_count_{0};
  bool active_{false};

  // Epoch state
  uint64_t current_epoch_{0};
  uint64_t epoch_start_seq_{0};

  // Metrics windows
  MetricsWindow state_window_;   // collects data for State message (reset at 4N/5)
  MetricsWindow reward_window_;  // collects data for reward computation (reset at N)

  // Prediction state
  std::future<Prediction> pending_prediction_;
  bool prediction_pending_{false};
  bool prediction_received_{false};
  Prediction received_prediction_;

  // Timeout tracking
  uint32_t current_timeout_ms_{0};
  uint32_t previous_timeout_ms_{0};
  bool timeout_changed_this_epoch_{false};

  // Feedback
  std::optional<std::pair<std::string, float>> pending_feedback_;

  mutable std::mutex mutex_;
  logging::Logger logger_{logging::getLogger("concord.kvbc.adaptive")};
};

}  // namespace concord::kvbc::strategy
