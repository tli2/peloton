//
// Created by tianyi on 7/6/18.
//

#ifndef PELOTON_CLIENT_TRANSACTION_TYPE_H
#define PELOTON_CLIENT_TRANSACTION_TYPE_H


namespace peloton {
namespace tcop {
enum class ClientTxnState {
  IDLE = 0,
  IMP_STARTED,
  EXP_STARTED,
  COMMITING,
  TO_ABORT,
};

enum class TxnEvent {
  IMP_START = 0,
  EXP_START,
  IMP_END,
  SOFT_ABORT,
  COMMIT,
  ABORT,
  NONE,
  TERMINATE,
};
}
}

#endif //PELOTON_CLIENT_TRANSACTION_TYPE_H
