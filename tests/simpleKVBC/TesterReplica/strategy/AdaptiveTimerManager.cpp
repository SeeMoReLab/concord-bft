#include "AdaptiveTimerManager.hpp"
#include "ReplicaConfig.hpp"

#include <google/protobuf/empty.pb.h>

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
                                         float latency_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!active_) return;

  auto record = [&](MetricsWindow& w) {
    w.slow_path_count += slow_count_delta;
    w.fast_path_count += fast_count_delta;
    w.total_latency_ms += latency_ms;
    w.count++;
    if (latency_ms > w.max_latency_ms) w.max_latency_ms = latency_ms;
    if (latency_ms < w.min_latency_ms) w.min_latency_ms = latency_ms;
  };

  record(state_window_);
  record(reward_window_);
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

  // At N/2: send feedback from previous epoch (if any) + Predict for current epoch
  if (txn_in_epoch == half) {
    // Send Learn for previous epoch if we have pending feedback
    if (pending_feedback_) {
      sendLearn(pending_feedback_->first, pending_feedback_->second);
      pending_feedback_ = std::nullopt;
    }

    // Send Predict with current state
    sendPredict();

    LOG_INFO(logger_, "Epoch " << current_epoch_ << ": sent Predict at txn " << txn_in_epoch
                               << " (seqNum=" << seqNum << ")");
  }

  // Between N/2 and 4N/5: poll for prediction
  if (txn_in_epoch > half && txn_in_epoch < apply_point && prediction_pending_) {
    pollPrediction();
  }

  // At 4N/5: apply timeout or give up polling
  if (txn_in_epoch == apply_point) {
    // Final poll attempt
    if (prediction_pending_) {
      pollPrediction();
    }

    if (prediction_received_) {
      uint32_t timeout = received_prediction_.action().timeout_milliseconds();
      if (timeout > 0) {
        previous_timeout_ms_ = current_timeout_ms_;
        current_timeout_ms_ = timeout;
        timeout_changed_this_epoch_ = true;
        new_timeout = timeout;

        // Write to ReplicaConfig so ReplicaImp picks it up
        bftEngine::ReplicaConfig::instance().set("concord.bft.adaptive.slowPathTimeout",
                                                 current_timeout_ms_);

        LOG_INFO(logger_, "Epoch " << current_epoch_ << ": applying timeout=" << timeout
                                   << "ms at txn " << txn_in_epoch);
      }
    } else {
      LOG_INFO(logger_, "Epoch " << current_epoch_
                                 << ": no prediction received by apply point, keeping timeout="
                                 << current_timeout_ms_ << "ms");
    }

    // Stop polling
    prediction_pending_ = false;
    prediction_received_ = false;

    // Reset state window — start collecting data for next epoch's State
    state_window_.reset();
  }

  // At N: record reward, advance epoch
  if (txn_in_epoch >= iteration_count_) {
    // Compute reward as throughput (transactions / second)
    float elapsed_ms = reward_window_.elapsedMs();
    float reward = elapsed_ms > 0
                       ? (static_cast<float>(reward_window_.count) * 1000.0f / elapsed_ms)
                       : 0.0f;

    // Use the timeout that was active this epoch for the feedback
    uint32_t epoch_timeout = timeout_changed_this_epoch_ ? current_timeout_ms_ : previous_timeout_ms_;
    std::string pred_id = received_prediction_.prediction_id();
    if (pred_id.empty()) {
      pred_id = "epoch_" + std::to_string(current_epoch_);
    }

    pending_feedback_ = std::make_pair(pred_id, reward);

    LOG_INFO(logger_, "Epoch " << current_epoch_ << " complete: reward=" << reward
                               << " timeout=" << epoch_timeout << "ms txns=" << reward_window_.count);

    // Advance epoch
    epoch_start_seq_ = seqNum;
    current_epoch_++;
    timeout_changed_this_epoch_ = false;
    reward_window_.reset();
    received_prediction_ = Prediction{};
  }

  return new_timeout;
}

void AdaptiveTimerManager::sendPredict() {
  State state;
  state.set_processed_transactions(state_window_.count);
  state.set_avg_message_latency(state_window_.avgLatency());
  state.set_max_message_latency(state_window_.max_latency_ms);
  state.set_min_message_latency(
      state_window_.min_latency_ms == std::numeric_limits<float>::max() ? 0 : state_window_.min_latency_ms);
  state.set_slow_path_count(state_window_.slow_path_count);
  state.set_fast_path_count(state_window_.fast_path_count);

  // Launch async RPC
  prediction_pending_ = true;
  prediction_received_ = false;

  auto stub = stub_.get();
  pending_prediction_ = std::async(std::launch::async, [stub, state]() {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    Prediction prediction;
    grpc::Status status = stub->Predict(&context, state, &prediction);
    if (!status.ok()) {
      throw std::runtime_error("Predict RPC failed: " + status.error_message());
    }
    return prediction;
  });
}

bool AdaptiveTimerManager::pollPrediction() {
  if (!prediction_pending_) return false;

  auto status = pending_prediction_.wait_for(std::chrono::seconds(0));
  if (status == std::future_status::ready) {
    try {
      received_prediction_ = pending_prediction_.get();
      prediction_received_ = true;
      prediction_pending_ = false;
      LOG_INFO(logger_, "Prediction received: id=" << received_prediction_.prediction_id()
                                                   << " timeout="
                                                   << received_prediction_.action().timeout_milliseconds()
                                                   << "ms");
      return true;
    } catch (const std::exception& e) {
      LOG_WARN(logger_, "Predict RPC failed: " << e.what());
      prediction_pending_ = false;
      prediction_received_ = false;
      return false;
    }
  }
  return false;
}

void AdaptiveTimerManager::sendLearn(const std::string& prediction_id, float reward) {
  Feedback feedback;
  feedback.set_prediction_id(prediction_id);
  feedback.set_reward(reward);

  // Fire and forget — async
  auto stub = stub_.get();
  std::thread([stub, feedback]() {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    google::protobuf::Empty empty;
    grpc::Status status = stub->Learn(&context, feedback, &empty);
    if (!status.ok()) {
      LOG_WARN(logging::getLogger("concord.kvbc.adaptive"),
               "Learn RPC failed: " << status.error_message());
    }
  }).detach();

  LOG_INFO(logger_, "Sent Learn: prediction_id=" << prediction_id << " reward=" << reward);
}

}  // namespace concord::kvbc::strategy
