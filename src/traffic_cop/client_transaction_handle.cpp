//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// client_transaction_handle.cpp
//
// Identification: src/traffic_cop/client_transaction_handle.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "traffic_cop/client_transaction_handle.h"

namespace peloton {
namespace tcop {

namespace {
#define DEF_TRANSITION_GRAPH                                          \
  ClientTxnHandle::TxnStateMachine::transition_result                   \
  ClientTxnHandle::TxnStateMachine::Delta_(ClientTxnState c, TxnEvent t) { \
    switch (c) {
#define DEFINE_STATE(s) \
  case ClientTxnState::s: {  \
    switch (t) {
#define ON(t)         \
  case TxnEvent::t: \
    return
#define SET_STATE_TO(s) \
  {                     \
    ClientTxnState::s,
#define AND_INVOKE(m)                         \
  ([](ClientTxnHandle &w) { return w.m(); }) \
  }                                           \
  ;
#define AND_INVOKE_WITH_ARG(m, arg)                         \
  ([](ClientTxnHandle &w) { return w.m(arg); }) \
  }                                           \
  ;
#define AND_IGNORE                            \
  return TxnEvent::NONE;                      \
  }                                           \
  ;

#define END_DEF                                       \
  default:                                            \
    throw std::runtime_error("undefined transition"); \
    }                                                 \
   }
#define END_STATE_DEF \
  ON(TERMINATE) SET_STATE_TO(IDLE) AND_INVOKE(AbortTxn) END_DEF

} // namespace
  // clang-format off
DEF_TRANSITION_GRAPH
    DEFINE_STATE(IDLE)
        ON(IMP_START) SET_STATE_TO(IMP_STARTED) AND_INVOKE(StartTxn)
        ON(EXP_START) SET_STATE_TO(EXP_STARTED) AND_INVOKE(StartTxn)
    END_STATE_DEF

    DEFINE_STATE(IMP_STARTED)
      // Ignored events
      ON(IMP_START) SET_STATE_TO(IMP_STARTED) AND_IGNORE
      ON(EXP_START) SET_STATE_TO(EXP_STARTED) AND_IGNORE
      // Handled events
      ON(IMP_END) SET_STATE_TO(IDLE) AND_INVOKE(EndTxn)
      ON(SOFT_ABORT) SET_STATE_TO(IDLE) AND_INVOKE(AbortTxn)
    END_STATE_DEF

    DEFINE_STATE(EXP_STARTED)
      // Ignored events
      ON(IMP_START) SET_STATE_TO(EXP_STARTED) AND_IGNORE
      ON(IMP_END) SET_STATE_TO(EXP_STARTED) AND_IGNORE
      // Handled events
      ON(COMMIT) SET_STATE_TO(COMMITING) AND_INVOKE(CommitTxn)
      ON(ABORT) SET_STATE_TO(IDLE) AND_INVOKE(AbortTxn)
      ON(SOFT_ABORT) SET_STATE_TO(TO_ABORT) AND_INVOKE(AbortTxn)
    END_STATE_DEF

    DEFINE_STATE(TO_ABORT)
      // Ignored events
      ON(IMP_START) SET_STATE_TO(TO_ABORT) AND_IGNORE
      ON(IMP_END) SET_STATE_TO(TO_ABORT) AND_IGNORE
      // Handled events
      ON(ABORT) SET_STATE_TO(IDLE) AND_IGNORE

    END_STATE_DEF

    DEFINE_STATE(COMMITING)
        // Handled events
        ON(COMMIT) SET_STATE_TO(IDLE) AND_IGNORE
        ON(ABORT) SET_STATE_TO(TO_ABORT) AND_INVOKE(AbortTxn)
    END_STATE_DEF
END_DEF
// clang-format-on

ClientTxnHandle::ClientTxnHandle()
    : txn_manager_(concurrency::TransactionManagerFactory::GetInstance()) {}

}

}
}
