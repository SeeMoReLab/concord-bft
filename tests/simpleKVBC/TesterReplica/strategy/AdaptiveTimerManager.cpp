#include "AdaptiveTimerManager.hpp"
#include "ReplicaConfig.hpp"

#include <google/protobuf/empty.pb.h>
#include <thread>

namespace concord::kvbc::strategy {

AdaptiveTimerManager& AdaptiveTimerManager::instance() {
  static AdaptiveTimerManager inst;
  return inst;
}

void AdaptiveTimerManager::init(const std::string& agent_addr, uint64_t iteration_count) {
  std::lock_guard<std::mutex> lock(mutex_);
  iteration_count_ = iteration_count;
  channel_ = grpc::CreateChannel(agent_addr, grpc::InsecureChannelCredentials());
  stub_ = LearningAgent::NewStub(channel_);
  active_ = true;
  LOG_INFO(logger_, "AdaptiveTimerManager initialized: addr=" << agent_addr
                                                              << " iterations=" << iteration_count);
}

bool AdaptiveTimerManager::isActive() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_;
}

void AdaptiveTimerManager::recordMetrics(uint32_t slow_count_delta,
                                         uint32_t fast_count_delta,
                                         float latency_ms,
                                         uint32_t batch_size) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!active_) return;

  auto record = [&](MetricsWindow& w) {
    w.slow_path_count += slow_count_delta;
    w.fast_path_count += fast_count_delta;
    w.total_latency_ms += latency_ms;
    w.total_transactions += batch_size;
    w.consensus_instance_count++;
    w.latency_samples_ms.push_back(latency_ms);
    w.batch_size_samples.push_back(static_cast<float>(batch_size));
  };

  record(state_window_);
  record(reward_window_);
}

void AdaptiveTimerManager::recordLeaderChange() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!active_) return;
  state_window_.leader_change_count++;
  reward_window_.leader_change_count++;
}

void AdaptiveTimerManager::recordRegencyChange() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!active_) return;
  state_window_.regency_change_count++;
  reward_window_.regency_change_count++;
}

std::optional<uint32_t> AdaptiveTimerManager::onTransaction(uint64_t seqNum) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!active_ || iteration_count_ == 0) return std::nullopt;

  // Initialize epoch start on first call
  if (current_epoch_ == 0 && epoch_start_seq_ == 0) {
    epoch_start_seq_ = seqNum;
    current_epoch_ = 1;
    state_window_.reset();
    reward_window_.reset();
    LOG_INFO(logger_, "Adaptive timer epoch 1 starting at seqNum=" << seqNum);
  }

  uint64_t txn_in_epoch = seqNum - epoch_start_seq_;
  uint64_t half = iteration_count_ / 2;
  uint64_t apply_point = (4 * iteration_count_) / 5;

  std::optional<uint32_t> new_timeout;

  // At N/2: send report (embedding reward from previous epoch) and start polling for timeout
  if (txn_in_epoch == half) {
    sendReport();
    LOG_INFO(logger_, "Epoch " << current_epoch_ << ": sent Report at txn " << txn_in_epoch
                               << " (seqNum=" << seqNum << ")");
  }

  // Between N/2 and 4N/5: poll for timeout recommendation
  if (txn_in_epoch > half && txn_in_epoch < apply_point && timeout_request_pending_) {
    pollTimeout();
  }

  // At 4N/5: apply timeout or give up polling
  if (txn_in_epoch == apply_point) {
    // Final poll attempt
    if (timeout_request_pending_) {
      pollTimeout();
    }

    if (timeout_received_) {
      if (!received_timeout_status_.has_timeout()) {
        LOG_WARN(logger_, "Epoch " << current_epoch_
                                   << ": READY status received but timeout field not populated");
      }
      const auto& timeout_msg = received_timeout_status_.timeout();
      bool any_changed = false;

      if (timeout_msg.has_sbft()) {
        const auto& sbft = timeout_msg.sbft();

        if (sbft.slow_path_timeout_milliseconds() > 0) {
          previous_timeout_ms_ = current_timeout_ms_;
          current_timeout_ms_ = sbft.slow_path_timeout_milliseconds();
          timeout_changed_this_epoch_ = true;
          new_timeout = current_timeout_ms_;
          bftEngine::ReplicaConfig::instance().set("concord.bft.adaptive.slowPathTimeout",
                                                   current_timeout_ms_);
          any_changed = true;
        }

        if (sbft.election_timeout_milliseconds() > 0) {
          bftEngine::ReplicaConfig::instance().set("concord.bft.adaptive.viewChangeTimeout",
                                                   sbft.election_timeout_milliseconds());
          any_changed = true;
        }

        if (sbft.batch_timeout_milliseconds() > 0) {
          bftEngine::ReplicaConfig::instance().set("concord.bft.adaptive.batchFlushTimeout",
                                                   sbft.batch_timeout_milliseconds());
          any_changed = true;
        }

        if (any_changed) {
          LOG_INFO(logger_,
                   "Epoch " << current_epoch_ << ": applying timeouts at txn " << txn_in_epoch
                             << " slow_path=" << sbft.slow_path_timeout_milliseconds()
                             << "ms election=" << sbft.election_timeout_milliseconds()
                             << "ms batch=" << sbft.batch_timeout_milliseconds() << "ms");
        }
      }
    } else {
      LOG_INFO(logger_, "Epoch " << current_epoch_
                                 << ": no timeout received by apply point, keeping timeout="
                                 << current_timeout_ms_ << "ms");
    }

    // Stop polling
    timeout_request_pending_ = false;
    timeout_received_ = false;

    // Reset state window — start collecting data for next epoch's Report
    state_window_.reset();
  }

  // At N: build Reward for next epoch, advance epoch
  if (txn_in_epoch >= iteration_count_) {
    uint32_t epoch_timeout = timeout_changed_this_epoch_ ? current_timeout_ms_ : previous_timeout_ms_;

    Reward reward;
    auto* sbft_reward = reward.mutable_sbft();
    sbft_reward->set_episode(static_cast<uint32_t>(current_epoch_));
    *sbft_reward->mutable_report() = buildReport(reward_window_);
    sbft_reward->mutable_timeout_used()->set_slow_path_timeout_milliseconds(epoch_timeout);
    pending_reward_ = std::move(reward);

    LOG_INFO(logger_, "Epoch " << current_epoch_ << " complete: timeout_used=" << epoch_timeout
                               << "ms txns=" << reward_window_.total_transactions);

    // Advance epoch
    epoch_start_seq_ = seqNum;
    current_epoch_++;
    timeout_changed_this_epoch_ = false;
    reward_window_.reset();
    received_timeout_status_ = TimeoutStatus{};
  }

  return new_timeout;
}

SbftReport AdaptiveTimerManager::buildReport(const MetricsWindow& w) const {
  SbftReport report;

  report.set_total_transactions(w.total_transactions);
  report.set_total_consensus_instances(w.consensus_instance_count);

  report.set_avg_consensus_latency_ms(w.avgLatency());
  report.set_p95_consensus_latency_ms(MetricsWindow::percentile(w.latency_samples_ms, 95.0f));
  report.set_p99_consensus_latency_ms(MetricsWindow::percentile(w.latency_samples_ms, 99.0f));

  float elapsed_ms = w.elapsedMs();
  if (elapsed_ms > 0 && w.total_transactions > 0)
    report.set_throughput_tps(w.total_transactions * 1000.0f / elapsed_ms);

  if (w.consensus_instance_count > 0)
    report.set_timeout_violation_rate(static_cast<float>(w.slow_path_count) /
                                      w.consensus_instance_count);

  report.set_avg_batch_size(w.avgBatchSize());
  report.set_p95_batch_size(MetricsWindow::percentile(w.batch_size_samples, 95.0f));

  report.set_leader_change_count(w.leader_change_count);
  report.set_regency_change_count(w.regency_change_count);

  return report;
}

void AdaptiveTimerManager::sendReport() {
  ReportLocal local;
  local.set_node_id(bftEngine::ReplicaConfig::instance().replicaId);
  local.set_episode(static_cast<uint32_t>(current_epoch_));
  local.set_protocol(PROTOCOL_SBFT);
  *local.mutable_sbft_state() = buildReport(state_window_);
  if (pending_reward_) {
    *local.mutable_reward() = *pending_reward_;
    pending_reward_ = std::nullopt;
  }

  // Fire-and-forget SendReport
  auto stub = stub_.get();
  std::thread([stub, local]() {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    google::protobuf::Empty empty;
    grpc::Status status = stub->SendReport(&context, local, &empty);
    if (!status.ok()) {
      LOG_WARN(logging::getLogger("concord.kvbc.adaptive"),
               "SendReport RPC failed: " << status.error_message());
    }
  }).detach();

  // Cancel any still-running GetTimeout loop from a previous epoch so that
  // overwriting pending_timeout_ below does not block (std::async futures
  // have a blocking destructor when launched with std::launch::async).
  if (cancel_token_) cancel_token_->store(true);

  // Start async GetTimeout polling loop with a fresh cancellation token
  cancel_token_ = std::make_shared<std::atomic<bool>>(false);
  timeout_request_pending_ = true;
  timeout_received_ = false;
  uint32_t episode = static_cast<uint32_t>(current_epoch_);
  auto cancel = cancel_token_;
  pending_timeout_ = std::async(std::launch::async, [stub, episode, cancel]() -> TimeoutStatus {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!cancel->load() && std::chrono::steady_clock::now() < deadline) {
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
      TimeoutRequest request;
      request.set_episode(episode);
      request.set_protocol(PROTOCOL_SBFT);
      TimeoutStatus status_resp;
      grpc::Status status = stub->GetTimeout(&context, request, &status_resp);
      if (!status.ok()) {
        LOG_WARN(logging::getLogger("concord.kvbc.adaptive"),
                 "GetTimeout RPC failed (will retry): " << status.error_message());
        if (!cancel->load()) std::this_thread::sleep_for(std::chrono::milliseconds(500));
        continue;
      }
      if (status_resp.status() == TimeoutStatus::READY) {
        return status_resp;
      }
      // NOT_RECEIVED or PENDING: retry after a short delay
      if (!cancel->load()) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    throw std::runtime_error("GetTimeout cancelled or timed out");
  });
}

bool AdaptiveTimerManager::pollTimeout() {
  if (!timeout_request_pending_) return false;

  auto status = pending_timeout_.wait_for(std::chrono::seconds(0));
  if (status == std::future_status::ready) {
    try {
      received_timeout_status_ = pending_timeout_.get();
      timeout_received_ = true;
      timeout_request_pending_ = false;
      const auto& t = received_timeout_status_.timeout();
      if (t.has_sbft()) {
        const auto& sbft = t.sbft();
        LOG_INFO(logger_, "Timeout received for episode=" << received_timeout_status_.episode()
                                                          << " slow_path="
                                                          << sbft.slow_path_timeout_milliseconds()
                                                          << "ms election="
                                                          << sbft.election_timeout_milliseconds()
                                                          << "ms batch="
                                                          << sbft.batch_timeout_milliseconds()
                                                          << "ms");
      }
      return true;
    } catch (const std::exception& e) {
      LOG_WARN(logger_, "GetTimeout failed: " << e.what());
      timeout_request_pending_ = false;
      timeout_received_ = false;
      return false;
    }
  }
  return false;
}

}  // namespace concord::kvbc::strategy
