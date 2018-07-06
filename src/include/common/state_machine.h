//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// state_machine.h
//
// Identification: src/include/common/state_machine.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <utility>
#include "logger.h"

namespace peloton {
/**
 * A state machine is defined to be a set of states, a set of symbols it
 * supports, and a function mapping each
 * state and symbol pair to the state it should transition to. i.e.
 * transition_graph = state * symbol -> state
 *
 * In addition to the transition system, our network state machine also needs
 * to perform actions. Actions are
 * defined as functions (lambdas, or closures, in various other languages) and
 * is promised to be invoked by the
 * state machine after each transition if registered in the transition graph.
 *
 * So the transition graph overall has type transition_graph = state * symbol
 * -> state
 */
template <typename State, typename Transition, typename Handle, typename Exception>
class StateMachine {
 public:
  StateMachine (State s) : current_state_(s) {};
  using action = Transition (*)(Handle &);
  using transition_result = std::pair<State, action>;
  /**
   * Runs the internal state machine, starting from the symbol given, until no
   * more
   * symbols are available.
   *
   * Each state of the state machine defines a map from a transition symbol to
   * an action
   * and the next state it should go to. The actions can either generate the
   * next symbol,
   * which means the state machine will continue to run on the generated
   * symbol, or signal
   * that there is no more symbols that can be generated, at which point the
   * state machine
   * will stop running and return, waiting for an external event (user
   * interaction, or system event)
   * to generate the next symbol.
   *
   * @param action starting symbol
   * @param handle the handling object to apply actions to
   */
  void Accept(Transition action, Handle &handle) {
    Transition next = action;
    while (next != Transition::NONE) {
      transition_result result = Delta_(current_state_, next);
      current_state_ = result.first;
      try {
        next = result.second(handle);
      } catch (Exception &e) {
        LOG_ERROR("%s\n", e.what());
        next = Transition::TERMINATE;
      }
    }
  }

 protected:
  /**
   * Current state of the state machine
   */
  State current_state_;
  /**
   * delta is the transition function that defines, for each state, its
   * behavior and the
   * next state it should go to.
   */
  virtual transition_result Delta_(State state, Transition transition);
};
}
