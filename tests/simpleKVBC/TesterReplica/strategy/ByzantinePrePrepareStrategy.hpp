#pragma once

#include "log/logger.hpp"
#include "TesterReplica/strategy/ByzantineStrategy.hpp"

namespace concord::kvbc::strategy {

// Byzantine strategy for PrePrepare messages that supports:
// - Proposal delay: adds configurable delay before sending PrePrepare
// - Skip fast path: forces CommitPath::SLOW in outgoing PrePrepare
// Both behaviors are driven dynamically by ByzantineFaultConfig timestamps.
class ByzantinePrePrepareStrategy : public IByzantineStrategy {
 public:
  explicit ByzantinePrePrepareStrategy(logging::Logger& logger) : logger_(logger) {}
  virtual ~ByzantinePrePrepareStrategy() = default;

  std::string getStrategyName() override;
  uint16_t getMessageCode() override;
  bool changeMessage(std::shared_ptr<bftEngine::impl::MessageBase>& msg) override;

 private:
  logging::Logger logger_;
};

}  // namespace concord::kvbc::strategy
