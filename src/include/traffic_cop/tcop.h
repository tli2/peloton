//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// t=cop.h
//
// Identification: src/include/traffic_cop/tcop.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <stack>
#include "catalog/column.h"
#include "executor/plan_executor.h"
#include "optimizer/abstract_optimizer.h"
#include "parser/postgresparser.h"
#include "parser/sql_statement.h"
#include "common/statement_cache.h"
#include "optimizer/optimizer.h"
namespace peloton {
namespace tcop {

// pair of txn ptr and the result so-far for that txn
// use a stack to support nested-txns
using TcopTxnState = std::pair<concurrency::TransactionContext *, ResultType>;

// TODO(Tianyu): Probably need a better name
// TODO(Tianyu): We can probably get rid of a bunch of fields from here
struct ClientProcessState {
  size_t thread_id_ = 0;
  bool is_queuing_ = false;
  std::string error_message_, db_name_ = DEFAULT_DB_NAME;
  std::vector<type::Value> param_values_;
  // This save currnet statement in the traffic cop
  std::shared_ptr<Statement> statement_;
  // The optimizer used for this connection
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer_{new optimizer::Optimizer()};
  // flag of single statement txn
  bool single_statement_txn_ = true;
  std::vector<PostgresDataFormat> result_format_;
  // flag of single statement txn
  std::vector<ResultValue> result_;
  std::stack<TcopTxnState> tcop_txn_state_;
  NetworkTransactionStateType txn_state_ = NetworkTransactionStateType::INVALID;
  bool skipped_stmt_ = false;
  std::string skipped_query_string_;
  QueryType skipped_query_type_ = QueryType::QUERY_INVALID;
  StatementCache statement_cache_;
  int rows_affected_ = 0;
  executor::ExecutionResult p_status_;

  // TODO(Tianyu): This is vile, get rid of this
  TcopTxnState &GetCurrentTxnState() {
    if (tcop_txn_state_.empty()) {
      static TcopTxnState
          default_state = std::make_pair(nullptr, ResultType::INVALID);
      return default_state;
    }
    return tcop_txn_state_.top();
  }

  // TODO(Tianyu): This is also vile, get rid of this. This is only used for testing
  void Reset() {
    thread_id_ = 0;
    is_queuing_ = false;
    error_message_ = "";
    db_name_ = DEFAULT_DB_NAME;
    param_values_.clear();
    statement_.reset();
    optimizer_.reset(new optimizer::Optimizer());
    single_statement_txn_ = false;
    result_format_.clear();
    result_.clear();
    tcop_txn_state_ = std::stack<TcopTxnState>();
    txn_state_ = NetworkTransactionStateType::INVALID;
    skipped_stmt_ = false;
    skipped_query_string_ = "";
    skipped_query_type_ = QueryType::QUERY_INVALID;
    statement_cache_.Clear();
    rows_affected_ = 0;
    p_status_ = executor::ExecutionResult();
  }
};

// TODO(Tianyu): We use an instance here in expectation that instance variables
// such as parser or others will be here when we refactor singletons, but Tcop
// should not have any Client specific states.
class Tcop {
 public:
  // TODO(Tianyu): Remove later
  inline static Tcop &GetInstance() {
    static Tcop tcop;
    return tcop;
  }

  inline std::unique_ptr<parser::SQLStatementList> ParseQuery(const std::string &query_string) {
    auto &peloton_parser = parser::PostgresParser::GetInstance();
    auto sql_stmt_list = peloton_parser.BuildParseTree(query_string);
    // When the query is empty(such as ";" or ";;", still valid),
    // the parse tree is empty, parser will return nullptr.
    if (sql_stmt_list != nullptr && !sql_stmt_list->is_valid)
      throw ParserException("Error Parsing SQL statement");
    return sql_stmt_list;
  }

  std::shared_ptr<Statement> PrepareStatement(ClientProcessState &state,
                                              const std::string &statement_name,
                                              const std::string &query_string,
                                              std::unique_ptr<parser::SQLStatementList> &&sql_stmt_list);

  ResultType ExecuteStatement(
      ClientProcessState &state,
      CallbackFunc callback);

  bool BindParamsForCachePlan(
      ClientProcessState &state,
      const std::vector<std::unique_ptr<expression::AbstractExpression>> &);

  std::vector<FieldInfo> GenerateTupleDescriptor(ClientProcessState &state,
                                                 parser::SQLStatement *select_stmt);

  static FieldInfo GetColumnFieldForValueType(std::string column_name,
                                              type::TypeId column_type);

  // Get all data tables from a TableRef.
  // For multi-way join
  // TODO(Bowei) still a HACK
  void GetTableColumns(ClientProcessState &state,
                       parser::TableRef *from_table,
                       std::vector<catalog::Column> &target_columns);

  void ExecuteStatementPlanGetResult(ClientProcessState &state);

  ResultType ExecuteStatementGetResult(ClientProcessState &state);

  void ProcessInvalidStatement(ClientProcessState &state);

  ResultType CommitQueryHelper(ClientProcessState &state);
  ResultType BeginQueryHelper(ClientProcessState &state);
  ResultType AbortQueryHelper(ClientProcessState &state);
  executor::ExecutionResult ExecuteHelper(ClientProcessState &state,
                                          CallbackFunc callback);

};

} // namespace tcop
} // namespace peloton