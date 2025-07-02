#include "memtx_engine.h"

#include "memtx_tx.h"
//#include "memtx_engine.h"

//static void
//memtx_engine_free(struct engine *engine)
//{
//	free(engine);
//}

void
memtx_engine_send_to_read_view(/*struct engine *engine, */struct txn *txn, int64_t psn)
{
	//(void)engine;
	memtx_tx_send_to_read_view(txn, psn);
}

void
memtx_engine_abort_with_conflict(/*struct engine *engine, */struct txn *txn)
{
	//(void)engine;
	memtx_tx_abort_with_conflict(txn);
}

void
memtx_engine_rollback_statement(/*struct engine *engine, */struct txn *txn, struct txn_stmt *stmt)
{
	//(void)engine;
	struct tuple *old_tuple = stmt->rollback_info.old_tuple;
	struct tuple *new_tuple = stmt->rollback_info.new_tuple;
	if (old_tuple == NULL && new_tuple == NULL)
		return;
	return memtx_tx_history_rollback_stmt(stmt);
}

void
memtx_engine_prepare(/*struct engine *engine, */struct txn *txn)
{
	struct txn_stmt *stmt;
	stailq_foreach_entry(stmt, &txn->stmts, next)
		memtx_tx_history_prepare_stmt(stmt);
}

void
memtx_engine_commit(/*struct engine *engine, */struct txn *txn)
{
	struct txn_stmt *stmt;
	stailq_foreach_entry(stmt, &txn->stmts, next)
		memtx_tx_history_commit_stmt(stmt);
}

//static const struct engine_vtab memtx_engine_vtab = {
//	/* .free = */ memtx_engine_free,
//	/* .prepare = */ memtx_engine_prepare,
//	/* .commit = */ memtx_engine_commit,
//	/* .rollback_statement = */ memtx_engine_rollback_statement,
//	/* .send_to_read_view = */ memtx_engine_send_to_read_view,
//	/* .abort_with_conflict = */ memtx_engine_abort_with_conflict,
//};
//
//struct memtx_engine *
//memtx_engine_new()
//{
//	struct memtx_engine *memtx = (struct memtx_engine *)malloc(sizeof(struct memtx_engine));
//	if (memtx == NULL) {
//		diag_set(OutOfMemory, sizeof(struct memtx_engine), "malloc", "struct memtx_engine");
//		return NULL;
//	}
//	memtx->base.vtab = &memtx_engine_vtab;
//	return memtx;
//}
