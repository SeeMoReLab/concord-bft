#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "adaptive_timers.grpc.pb.h"

#include "log/logger.hpp"

namespace concord::kvbc::strategy {

struct MetricsWindow {
  // Transaction and consensus instance counts
  uint32_t total_transactions{0};       // sum of batch sizes across all consensus instances
  uint32_t consensus_instance_count{0}; // number of consensus rounds

  // Latency tracking (per consensus instance, milliseconds)
  float total_latency_ms{0};
  std::vector<float> latency_samples_ms;

  // Batch size tracking (per consensus instance)
  std::vector<float> batch_size_samples;

  // Path counts (per consensus instance)
  uint32_t slow_path_count{0};
  uint32_t fast_path_count{0};

  // Leadership dynamics
  uint32_t leader_change_count{0};
  uint32_t regency_change_count{0};

  std::chrono::steady_clock::time_point start_time{std::chrono::steady_clock::now()};

  void reset() {
    total_transactions = 0;
    consensus_instance_count = 0;
    total_latency_ms = 0;
    latency_samples_ms.clear();
    batch_size_samples.clear();
    slow_path_count = 0;
    fast_path_count = 0;
    leader_change_count = 0;
    regency_change_count = 0;
    start_time = std::chrono::steady_clock::now();
  }

  float avgLatency() const {
    return consensus_instance_count > 0 ? total_latency_ms / consensus_instance_count : 0;
  }

  float avgBatchSize() const {
    return consensus_instance_count > 0
               ? static_cast<float>(total_transactions) / consensus_instance_count
               : 0;
  }

  float elapsedMs() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start_time)
        .count();
  }

  // Returns the p-th percentile (e.g. 95.0, 99.0) of the given sample set.
  static float percentile(const std::vector<float>& samples, float p) {
    if (samples.empty()) return 0.0f;
    std::vector<float> sorted(samples);
    std::sort(sorted.begin(), sorted.end());
    float idx = (p / 100.0f) * static_cast<float>(sorted.size() - 1);
    auto lo = static_cast<size_t>(idx);
    auto hi = std::min(lo + 1, sorted.size() - 1);
    float frac = idx - static_cast<float>(lo);
    return sorted[lo] * (1.0f - frac) + sorted[hi] * frac;
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

  // Feed per-consensus-instance metrics. batch_size is the number of client
  // requests in the PrePrepare; latency_ms is end-to-end consensus latency.
  void recordMetrics(uint32_t slow_count_delta,
                     uint32_t fast_count_delta,
                     float latency_ms,
                     uint32_t batch_size);

  // Increment leadership-dynamics counters (call on view-change / regency-change events).
  void recordLeaderChange();
  void recordRegencyChange();

 private:
  AdaptiveTimerManager() = default;
  AdaptiveTimerManager(const AdaptiveTimerManager&) = delete;
  AdaptiveTimerManager& operator=(const AdaptiveTimerManager&) = delete;

  void sendReport();
  bool pollTimeout();
  SbftReport buildReport(const MetricsWindow& w) const;

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
  MetricsWindow state_window_;   // collects data for Report message (reset at 4N/5)
  MetricsWindow reward_window_;  // collects data for Reward message (reset at N)

  // Timeout polling state
  std::future<TimeoutStatus> pending_timeout_;
  std::shared_ptr<std::atomic<bool>> cancel_token_;
  bool timeout_request_pending_{false};
  bool timeout_received_{false};
  TimeoutStatus received_timeout_status_;

  // Timeout tracking
  uint32_t current_timeout_ms_{0};
  uint32_t previous_timeout_ms_{0};
  bool timeout_changed_this_epoch_{false};

  // Reward to embed in the next SendReport
  std::optional<Reward> pending_reward_;

  mutable std::mutex mutex_;
  logging::Logger logger_{logging::getLogger("concord.kvbc.adaptive")};
};

}  // namespace concord::kvbc::strategy
