//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// client_transaction_handle.h
//
// Identification: src/include/traffic_cop/client_transaction_handle.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrency/transaction_manager_factory.h"
#include "common/state_machine.h"

namespace peloton {
namespace tcop {


class ClientTxnHandle {
  typedef concurrency::TransactionContext TxnContext;

 public:
  ClientTxnHandle();

  inline TxnContext *ImplicitStart(const size_t thread_id = 0) {
    this->thread_id_ = thread_id;
    state_machine_.Accept(TxnEvent::IMP_START, *this);
    return this->curr_txn_;
  }

  inline void ImplicitEnd() {
    state_machine_.Accept(TxnEvent::IMP_END, *this);
  }

  inline TxnContext *ExplicitStart(const size_t thread_id = 0) {
    this->thread_id_ = thread_id;
    state_machine_.Accept(TxnEvent::EXP_START, *this);
    return this->curr_txn_;
  }

  inline bool ExplicitCommit() {
    state_machine_.Accept(TxnEvent::COMMIT, *this);
    return (state_machine_.GetState() == ClientTxnState::IDLE);
  }

  inline void ExplicitAbort() {
    state_machine_.Accept(TxnEvent::ABORT, *this);
  }

  void void SoftAbort() {
    state_machine_.Accept(TxnEvent::SOFT_ABORT, *this);
  }


 private:
  enum class ClientTxnState {
    IDLE = 0,
    IMP_START,
    IMP_END,
    EXP_START,
    COMMITING,
    TO_ABORT,
    NONE,
    TERMINATE,
  };

  enum class TxnEvent {
    IMP_START = 0,
    EXP_START,
    IMP_END,
    SOFT_ABORT,
    COMMIT,
    ABORT
  };

  class TxnStateMachine : StateMachine<ClientTxnState, TxnEvent, ClientTxnHandle, TransactionException> {
   public:
    TxnStateMachine() : StateMachine(ClientTxnState::IDLE) {}

  };

 private:
  TxnContext *curr_txn_;
  concurrency::TransactionManager &manager;
  TxnStateMachine state_machine_;

  size_t thread_id_ = 0;
};

}
}