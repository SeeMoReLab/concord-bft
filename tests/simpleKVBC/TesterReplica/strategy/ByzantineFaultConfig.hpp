#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace concord::kvbc::strategy {

struct FaultEntry {
  uint64_t time_ms{0};
  uint64_t delay_propose{0};
  bool skip_fast_path{false};
  bool delay_fast_path{false};
};

class ByzantineFaultConfig {
 public:
  static ByzantineFaultConfig& instance();

  void load(const std::string& json_file_path);
  void start();
  FaultEntry getCurrentConfig() const;
  bool isLoaded() const;
  bool hasAnyDelayPropose() const;
  bool hasAnySkipFastPath() const;
  bool hasAnyDelayFastPath() const;

 private:
  ByzantineFaultConfig() = default;
  ByzantineFaultConfig(const ByzantineFaultConfig&) = delete;
  ByzantineFaultConfig& operator=(const ByzantineFaultConfig&) = delete;

  std::vector<FaultEntry> entries_;
  std::chrono::steady_clock::time_point start_time_;
  mutable std::mutex mutex_;
  mutable size_t current_index_{0};
  bool loaded_{false};
  bool started_{false};
};

}  // namespace concord::kvbc::strategy
