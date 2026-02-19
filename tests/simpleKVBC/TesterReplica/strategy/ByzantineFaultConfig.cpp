#include "ByzantineFaultConfig.hpp"

#include <algorithm>
#include <fstream>
#include <stdexcept>

#include <nlohmann/json.hpp>

namespace concord::kvbc::strategy {

ByzantineFaultConfig& ByzantineFaultConfig::instance() {
  static ByzantineFaultConfig inst;
  return inst;
}

void ByzantineFaultConfig::load(const std::string& json_file_path) {
  std::lock_guard<std::mutex> lock(mutex_);

  std::ifstream file(json_file_path);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open byzantine fault config: " + json_file_path);
  }

  nlohmann::json j;
  file >> j;

  if (!j.is_array()) {
    throw std::runtime_error("Byzantine fault config must be a JSON array");
  }

  entries_.clear();
  current_index_ = 0;
  for (const auto& entry : j) {
    FaultEntry fe;
    fe.time_ms = entry.value("time", uint64_t{0});
    fe.delay_propose = entry.value("delay_propose", uint64_t{0});
    fe.skip_fast_path = entry.value("skip_fast_path", false);
    fe.delay_fast_path = entry.value("delay_fast_path", false);
    entries_.push_back(fe);
  }

  std::sort(entries_.begin(), entries_.end(),
            [](const FaultEntry& a, const FaultEntry& b) { return a.time_ms < b.time_ms; });

  loaded_ = true;
}

void ByzantineFaultConfig::start() {
  std::lock_guard<std::mutex> lock(mutex_);
  start_time_ = std::chrono::steady_clock::now();
  current_index_ = 0;
  started_ = true;
}

FaultEntry ByzantineFaultConfig::getCurrentConfig() const {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!loaded_ || !started_ || entries_.empty()) {
    return FaultEntry{};
  }

  auto now = std::chrono::steady_clock::now();
  auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time_).count();

  // Advance from the last applied index, since time only moves forward
  while (current_index_ + 1 < entries_.size() &&
         static_cast<int64_t>(entries_[current_index_ + 1].time_ms) <= elapsed_ms) {
    ++current_index_;
  }
  return entries_[current_index_];
}

bool ByzantineFaultConfig::isLoaded() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return loaded_;
}

bool ByzantineFaultConfig::hasAnyDelayPropose() const {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& e : entries_) {
    if (e.delay_propose > 0) return true;
  }
  return false;
}

bool ByzantineFaultConfig::hasAnySkipFastPath() const {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& e : entries_) {
    if (e.skip_fast_path) return true;
  }
  return false;
}

bool ByzantineFaultConfig::hasAnyDelayFastPath() const {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& e : entries_) {
    if (e.delay_fast_path) return true;
  }
  return false;
}

}  // namespace concord::kvbc::strategy
