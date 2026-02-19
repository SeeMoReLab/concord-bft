#include "ByzantinePrePrepareStrategy.hpp"
#include "ByzantineFaultConfig.hpp"
#include "AdaptiveTimerManager.hpp"
#include "StrategyUtils.hpp"

#include "messages/MsgCode.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "ReplicaConfig.hpp"

#include <chrono>
#include <thread>

namespace concord::kvbc::strategy {

std::string ByzantinePrePrepareStrategy::getStrategyName() {
  return CLASSNAME(ByzantinePrePrepareStrategy);
}

uint16_t ByzantinePrePrepareStrategy::getMessageCode() {
  return static_cast<uint16_t>(bftEngine::impl::MsgCode::PrePrepare);
}

bool ByzantinePrePrepareStrategy::changeMessage(std::shared_ptr<bftEngine::impl::MessageBase>& msg) {
  auto config = ByzantineFaultConfig::instance().getCurrentConfig();
  bool changed = false;

  if (config.delay_propose > 0) {
    LOG_INFO(logger_, "Byzantine: delaying PrePrepare by " << config.delay_propose << " ms");
    std::this_thread::sleep_for(std::chrono::milliseconds(config.delay_propose));
  }

  if (config.skip_fast_path) {
    auto& pp = static_cast<bftEngine::impl::PrePrepareMsg&>(*msg);
    LOG_INFO(logger_, "Byzantine: forcing CommitPath::SLOW on PrePrepare seqNum=" << pp.seqNumber());
    pp.updateView(pp.viewNumber(), bftEngine::impl::CommitPath::SLOW);
    changed = true;
  }

  // Dynamically update delayFastPath so ReplicaImp picks up the current phase
  bftEngine::ReplicaConfig::instance().set("concord.bft.byz.delayFastPath", config.delay_fast_path);

  // Adaptive timer: feed transaction to the epoch-based learning loop
  if (AdaptiveTimerManager::instance().isActive()) {
    auto& pp = static_cast<bftEngine::impl::PrePrepareMsg&>(*msg);
    AdaptiveTimerManager::instance().onTransaction(pp.seqNumber());
  }

  return changed;
}

}  // namespace concord::kvbc::strategy
