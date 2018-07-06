//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tcop.h
//
// Identification: src/include/traffic_cop/tcop.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "threadpool/mono_queue_pool.h"
#include "planner/plan_util.h"
#include "binder/bind_node_visitor.h"
#include "traffic_cop/tcop.h"
#include "expression/expression_util.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace tcop {

std::shared_ptr<Statement> Tcop::PrepareStatement(ClientProcessState &state,
                                                  const std::string &statement_name,
                                                  const std::string &query_string,
                                                  std::unique_ptr<parser::SQLStatementList> &&sql_stmt_list) {
  LOG_TRACE("Prepare Statement query: %s", query_string.c_str());

  // Empty statement
  // TODO (Tianyi) Read through the parser code to see if this is appropriate
  if (sql_stmt_list == nullptr || sql_stmt_list->GetNumStatements() == 0)
    // TODO (Tianyi) Do we need another query type called QUERY_EMPTY?
    return std::make_shared<Statement>(statement_name,
                                       QueryType::QUERY_INVALID,
                                       query_string,
                                       std::move(sql_stmt_list));

  StatementType stmt_type = sql_stmt_list->GetStatement(0)->GetType();
  QueryType query_type =
      StatementTypeToQueryType(stmt_type, sql_stmt_list->GetStatement(0));
  auto statement = std::make_shared<Statement>(statement_name,
                                               query_type,
                                               query_string,
                                               std::move(sql_stmt_list));

  // for general queries, we need to check if the current txn is going to
  // be aborted
  if (query_type != QueryType::QUERY_ROLLBACK && state.txn_handle_.ToAbort()) {
    state.error_message_ = "Current transaction is going to be aborted.";
    return nullptr;
  }

  auto txn = state.txn_handle_.ImplicitStart();

  if (settings::SettingsManager::GetBool(settings::SettingId::brain)) {
    state.txn_handle_.GetTxn()->AddQueryString(query_string.c_str());
  }

  // TODO(Tianyi) Move Statement Planing into Statement's method
  // to increase coherence
  try {
    // Run binder
    auto bind_node_visitor = binder::BindNodeVisitor(
        txn, state.db_name_);
    bind_node_visitor.BindNameToNode(
        statement->GetStmtParseTreeList()->GetStatement(0));
    auto plan = state.optimizer_->BuildPelotonPlanTree(
        statement->GetStmtParseTreeList(), txn);
    statement->SetPlanTree(plan);
    // Get the tables that our plan references so that we know how to
    // invalidate it at a later point when the catalog changes
    const std::set<oid_t> table_oids =
        planner::PlanUtil::GetTablesReferenced(plan.get());
    statement->SetReferencedTables(table_oids);

    if (query_type == QueryType::QUERY_SELECT) {
      auto tuple_descriptor = GenerateTupleDescriptor(state,
          statement->GetStmtParseTreeList()->GetStatement(0));
      statement->SetTupleDescriptor(tuple_descriptor);
      LOG_TRACE("select query, finish setting");
    }
  } catch (Exception &e) {
    state.txn_handle_.SoftAbort();
    state.error_message_ = e.what();
    tcop::Tcop::GetInstance().ProcessInvalidStatement(state);
    return nullptr;
  }

#ifdef LOG_DEBUG_ENABLED
  if (statement->GetPlanTree().get() != nullptr) {
    LOG_TRACE("Statement Prepared: %s", statement->GetInfo().c_str());
    LOG_TRACE("%s", statement->GetPlanTree().get()->GetInfo().c_str());
  }
#endif
  return statement;
}

ResultType Tcop::ExecuteStatement(ClientProcessState &state,
                                  CallbackFunc callback) {

  LOG_TRACE("Execute Statement of name: %s",
            state.statement_->GetStatementName().c_str());
  LOG_TRACE("Execute Statement of query: %s",
            state.statement_->GetQueryString().c_str());
  LOG_TRACE("Execute Statement Plan:\n%s",
            planner::PlanUtil::GetInfo(state.statement_->GetPlanTree().get()).c_str());
  LOG_TRACE("Execute Statement Query Type: %s",
            state.statement_->GetQueryTypeString().c_str());
  LOG_TRACE("----QueryType: %d--------",
            static_cast<int>(state.statement_->GetQueryType()));

  try {
    switch (state.statement_->GetQueryType()) {
      case QueryType::QUERY_BEGIN:return BeginQueryHelper(state);
      case QueryType::QUERY_COMMIT:return CommitQueryHelper(state);
      case QueryType::QUERY_ROLLBACK:return AbortQueryHelper(state);
      default:
        if (state.txn_handle_.ToAbort()) {
          state.error_message_ = "Current transaction is going to be aborted.";
          return ResultType::TO_ABORT;
        }
        // The statement may be out of date
        // It needs to be replan
        auto txn = state.txn_handle_.ImplicitStart();
        if (state.statement_->GetNeedsReplan()) {
          // TODO(Tianyi) Move Statement Replan into Statement's method
          // to increase coherence
          auto bind_node_visitor = binder::BindNodeVisitor(
              txn, state.db_name_);
          bind_node_visitor.BindNameToNode(
              state.statement_->GetStmtParseTreeList()->GetStatement(0));
          auto plan = state.optimizer_->BuildPelotonPlanTree(
              state.statement_->GetStmtParseTreeList(), txn);
          state.statement_->SetPlanTree(plan);
          state.statement_->SetNeedsReplan(true);
        }

        ExecuteHelper(state, callback);
        if (state.is_queuing_)
          return ResultType::QUEUING;
        else
          return ExecuteStatementGetResult(state);
    }
  } catch (Exception &e) {
    state.error_message_ = e.what();
    return ResultType::FAILURE;
  }
}

bool Tcop::BindParamsForCachePlan(ClientProcessState &state,
                                  const std::vector<std::unique_ptr<expression::AbstractExpression>> &exprs) {
  if (state.txn_handle_.ToAbort()) {
    state.error_message_ = "Current transaction is going to be aborted.";
    return nullptr;
  }

  auto txn = state.txn_handle_.ImplicitStart();

  // Run binder
  auto bind_node_visitor =
      binder::BindNodeVisitor(txn, state.db_name_);

  std::vector<type::Value> param_values;
  for (const auto &expr :exprs) {
    if (!expression::ExpressionUtil::IsValidStaticExpression(expr.get())) {
      state.error_message_ = "Invalid Expression Type";
      state.txn_handle_.SoftAbort();
      return false;
    }
    expr->Accept(&bind_node_visitor);
    // TODO(Yuchen): need better check for nullptr argument
    param_values.push_back(expr->Evaluate(nullptr, nullptr, nullptr));
  }
  if (!param_values.empty()) {
    state.statement_->GetPlanTree()->SetParameterValues(&param_values);
  }
  state.param_values_ = param_values;
  return true;
}

std::vector<FieldInfo> Tcop::GenerateTupleDescriptor(ClientProcessState &state,
                                                     parser::SQLStatement *sql_stmt) {
  std::vector<FieldInfo> tuple_descriptor;
  if (sql_stmt->GetType() != StatementType::SELECT) return tuple_descriptor;
  auto select_stmt = (parser::SelectStatement *) sql_stmt;

  // TODO(Bowei): this is a hack which I don't have time to fix now
  // but it replaces a worse hack that was here before
  // What should happen here is that plan nodes should store
  // the schema of their expected results and here we should just read
  // it and put it in the tuple descriptor

  // Get the columns information and set up
  // the columns description for the returned results
  // Set up the table
  std::vector<catalog::Column> all_columns;

  // Check if query only has one Table
  // Example : SELECT * FROM A;
  GetTableColumns(state, select_stmt->from_table.get(), all_columns);

  int count = 0;
  for (auto &expr : select_stmt->select_list) {
    count++;
    if (expr->GetExpressionType() == ExpressionType::STAR) {
      for (const auto &column : all_columns) {
        tuple_descriptor.push_back(
            GetColumnFieldForValueType(column.GetName(), column.GetType()));
      }
    } else {
      std::string col_name;
      if (expr->alias.empty()) {
        col_name = expr->expr_name_.empty()
                   ? std::string("expr") + std::to_string(count)
                   : expr->expr_name_;
      } else {
        col_name = expr->alias;
      }
      tuple_descriptor.push_back(
          GetColumnFieldForValueType(col_name, expr->GetValueType()));
    }
  }

  return tuple_descriptor;
}

FieldInfo Tcop::GetColumnFieldForValueType(std::string column_name,
                                           type::TypeId column_type) {
  PostgresValueType field_type;
  size_t field_size;
  switch (column_type) {
    case type::TypeId::BOOLEAN:
    case type::TypeId::TINYINT: {
      field_type = PostgresValueType::BOOLEAN;
      field_size = 1;
      break;
    }
    case type::TypeId::SMALLINT: {
      field_type = PostgresValueType::SMALLINT;
      field_size = 2;
      break;
    }
    case type::TypeId::INTEGER: {
      field_type = PostgresValueType::INTEGER;
      field_size = 4;
      break;
    }
    case type::TypeId::BIGINT: {
      field_type = PostgresValueType::BIGINT;
      field_size = 8;
      break;
    }
    case type::TypeId::DECIMAL: {
      field_type = PostgresValueType::DOUBLE;
      field_size = 8;
      break;
    }
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY: {
      field_type = PostgresValueType::TEXT;
      field_size = 255;
      break;
    }
    case type::TypeId::DATE: {
      field_type = PostgresValueType::DATE;
      field_size = 4;
      break;
    }
    case type::TypeId::TIMESTAMP: {
      field_type = PostgresValueType::TIMESTAMPS;
      field_size = 64;  // FIXME: Bytes???
      break;
    }
    default: {
      // Type not Identified
      LOG_ERROR("Unrecognized field type '%s' for field '%s'",
                TypeIdToString(column_type).c_str(), column_name.c_str());
      field_type = PostgresValueType::TEXT;
      field_size = 255;
      break;
    }
  }
  // HACK: Convert the type into a oid_t
  // This ugly and I don't like it one bit...
  return std::make_tuple(column_name, static_cast<oid_t>(field_type),
                         field_size);
}

void Tcop::GetTableColumns(ClientProcessState &state,
                           parser::TableRef *from_table,
                           std::vector<catalog::Column> &target_columns) {
  if (from_table == nullptr) return;

  // Query derived table
  if (from_table->select != nullptr) {
    for (auto &expr : from_table->select->select_list) {
      if (expr->GetExpressionType() == ExpressionType::STAR)
        GetTableColumns(state, from_table->select->from_table.get(), target_columns);
      else
        target_columns.emplace_back(expr->GetValueType(), 0,
                                    expr->GetExpressionName());
    }
  } else if (from_table->list.empty()) {
    if (from_table->join == nullptr) {
      auto columns =
          catalog::Catalog::GetInstance()->GetTableWithName(
              state.txn_handle_.GetTxn(),
              from_table->GetDatabaseName(),
              from_table->GetSchemaName(),
              from_table->GetTableName())
              ->GetSchema()
              ->GetColumns();
      target_columns.insert(target_columns.end(), columns.begin(),
                            columns.end());
    } else {
      GetTableColumns(state, from_table->join->left.get(), target_columns);
      GetTableColumns(state, from_table->join->right.get(), target_columns);
    }
  }
    // Query has multiple tables. Recursively add all tables
  else
    for (auto &table : from_table->list)
      GetTableColumns(state, table.get(), target_columns);
}

void Tcop::ExecuteStatementPlanGetResult(ClientProcessState &state) {
  if (state.p_status_.m_result == ResultType::FAILURE) {
    state.txn_handle_.SoftAbort();
    return;
  } else {  // execution SUCCESS
    if (!state.txn_handle_.ImplicitEnd()) {
      state.p_status_.m_result = ResultType::ABORTED;
    }
  }
}

ResultType Tcop::ExecuteStatementGetResult(ClientProcessState &state) {
  LOG_TRACE("Statement executed. Result: %s",
           ResultTypeToString(state.p_status_.m_result).c_str());
  state.rows_affected_ = state.p_status_.m_processed;
  LOG_TRACE("rows_changed %d", state.p_status_.m_processed);
  state.is_queuing_ = false;
  return state.p_status_.m_result;
}

void Tcop::ProcessInvalidStatement(ClientProcessState &state) {
  state.txn_handle_.SoftAbort();
}

ResultType Tcop::CommitQueryHelper(ClientProcessState &state) {
  if (state.txn_handle_.CanCommit()) {
    bool success = state.txn_handle_.ExplicitCommit();
    if (success)  return ResultType::SUCCESS;
    else  return ResultType::FAILURE;
  } else {
    // TODO think of a better error message
    state.error_message_ = "Cannot commit";
    return ResultType::NOOP;
  }
}

ResultType Tcop::BeginQueryHelper(ClientProcessState &state) {
  if (state.txn_handle_.CanBegin()) {
    state.txn_handle_.ExplicitStart(state.thread_id_);
    return ResultType::SUCCESS;
  } else {
    return ResultType::FAILURE;
  }
}

ResultType Tcop::AbortQueryHelper(ClientProcessState &state) {
  if (state.txn_handle_.CanAbort()) {
    state.txn_handle_.ExplicitAbort();
    return ResultType::ABORTED;
  } else {
    // TODO Think of a way to pass the error message
    state.error_message_ = "Cannot abort when there is no txn";
    return ResultType::NOOP;
  }
}

executor::ExecutionResult Tcop::ExecuteHelper(ClientProcessState &state,
                                              CallbackFunc callback) {
  auto txn = state.txn_handle_.GetTxn();

  auto on_complete = [callback, &state](executor::ExecutionResult p_status,
                         std::vector<ResultValue> &&values) {
    state.p_status_ = p_status;
    // TODO (Tianyi) I would make a decision on keeping one of p_status or
    // error_message in my next PR
    state.error_message_ = std::move(p_status.m_error_message);
    state.result_ = std::move(values);
    callback();
  };
  // TODO(Tianyu): Eliminate this copy, which is here to coerce the type
  std::vector<int> formats;
  for (auto format : state.result_format_)
    formats.push_back((int) format);

  auto &pool = threadpool::MonoQueuePool::GetInstance();
  pool.SubmitTask([on_complete, txn, formats, &state] {
    executor::PlanExecutor::ExecutePlan(state.statement_->GetPlanTree(),
                                        txn,
                                        state.param_values_,
                                        formats,
                                        on_complete);
  });

  state.is_queuing_ = true;

  LOG_TRACE("Check Tcop_txn_state Size After ExecuteHelper %lu",
            state.tcop_txn_state_.size());
  return state.p_status_;
}

} // namespace tcop
} // namespace peloton