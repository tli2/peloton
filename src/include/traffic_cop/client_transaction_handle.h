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
#include "traffic_cop/client_transaction_type.h"
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

  inline bool ImplicitEnd() {
    state_machine_.Accept(TxnEvent::IMP_END, *this);
    return txn_commited_;
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

  inline void SoftAbort() {
    state_machine_.Accept(TxnEvent::SOFT_ABORT, *this);
  }

  inline bool ToAbort() {
    return (state_machine_.GetState() == ClientTxnState::TO_ABORT);
  }

  inline bool CanCommit() {
    return (state_machine_.GetState() == ClientTxnState::EXP_STARTED);
  }

  inline bool CanBegin() {
    return (state_machine_.GetState() == ClientTxnState::IDLE ||
            state_machine_.GetState() == ClientTxnState::IMP_STARTED);
  }

  inline bool CanAbort() {
    return (state_machine_.GetState() == ClientTxnState::TO_ABORT ||
            state_machine_.GetState() == ClientTxnState::EXP_STARTED);
  }

  inline TxnContext *GetTxn() {
    return this->curr_txn_;
  }


 private:
  class TxnStateMachine : StateMachine<ClientTxnState, TxnEvent, ClientTxnHandle, TransactionException> {
   public:
    TxnStateMachine() : StateMachine(ClientTxnState::IDLE) {}
   private:
    transition_result Delta_(ClientTxnState state, TxnEvent event) override;
  };

 private:
  TxnContext *curr_txn_;
  concurrency::TransactionManager &txn_manager_;
  TxnStateMachine state_machine_;

  size_t thread_id_ = 0;

  bool txn_commited_ = false;

 private:

  inline TxnEvent StartTxn() {
    this->curr_txn_ = txn_manager_.BeginTransaction(this->thread_id_);
    return TxnEvent::NONE;
  }

  inline TxnEvent AbortTxn() {
    txn_manager_.AbortTransaction(this->curr_txn_);
    txn_commited_ = false;
    return TxnEvent::NONE;
  }

  inline TxnEvent CommitTxn() {
    auto result = txn_manager_.CommitTransaction(this->curr_txn_);
    if (result == ResultType::SUCCESS) {
      txn_commited_ = true;
      return TxnEvent::COMMIT;
    } else {  // ResultType::Failure
      txn_commited_ = false;
      return TxnEvent::ABORT;
    }
  }

  inline TxnEvent EndTxn() {
    if (CommitTxn() == TxnEvent::COMMIT) {
      return TxnEvent::NONE;
    }
    AbortTxn();
    return TxnEvent::NONE;
  }

};

}
}