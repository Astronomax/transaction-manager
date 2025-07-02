#pragma once

#include "txn.h"
#include "stdint.h"

void
memtx_engine_send_to_read_view(/*struct engine *engine, */struct txn *txn, int64_t psn);

void
memtx_engine_abort_with_conflict(/*struct engine *engine, */struct txn *txn);

void
memtx_engine_rollback_statement(/*struct engine *engine, */struct txn *txn, struct txn_stmt *stmt);

void
memtx_engine_prepare(/*struct engine *engine, */struct txn *txn);

void
memtx_engine_commit(/*struct engine *engine, */struct txn *txn);
