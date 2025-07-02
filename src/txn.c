#include "txn.h"
#include "memtx_engine.h"
#include "memtx_tx.h"
#include "assert.h"
#include "stdbool.h"

/**
 * Incremental counter for psn (prepare sequence number) of a transaction.
 * The next prepared transaction will get psn == txn_next_psn++.
 * See also struct txn::psn.
 */
int64_t txn_next_psn = TXN_MIN_PSN;

RLIST_HEAD(txns);

/** Initialize a new stmt object within txn. */
static struct txn_stmt *
txn_stmt_new(struct txn *txn)
{
	size_t size;
	struct txn_stmt *stmt = (struct txn_stmt *)malloc(sizeof(struct txn_stmt));
	if (stmt == NULL) {
		fprintf(stderr, "Failed to allocate %u bytes in %s for %s", sizeof(struct txn_stmt), "malloc", "stmt");
		return NULL;
	}

	/* Initialize members explicitly to save time on memset() */
	stmt->txn = in_txn();
	stmt->space = NULL;
	stmt->old_tuple = NULL;
	stmt->new_tuple = NULL;
	stmt->rollback_info.old_tuple = NULL;
	stmt->rollback_info.new_tuple = NULL;
	stmt->add_story = NULL;
	stmt->del_story = NULL;
	stmt->next_in_del_list = NULL;
	stmt->is_own_change = false;
	return stmt;
}

static inline void
txn_stmt_destroy(struct txn_stmt *stmt)
{
	assert(stmt->add_story == NULL && stmt->del_story == NULL);
}

void
txn_stmt_prepare_rollback_info(struct txn_stmt *stmt, struct tuple *old_tuple, struct tuple *new_tuple)
{
	stmt->rollback_info.old_tuple = old_tuple;
	stmt->rollback_info.new_tuple = new_tuple;
}

void
txn_send_to_read_view(struct txn *txn, int64_t psn)
{
	assert(psn >= TXN_MIN_PSN);
	if (txn->status == TXN_ABORTED)
		return;
	assert(txn->status == TXN_INPROGRESS || txn->status == TXN_IN_READ_VIEW);
	//assert(txn_has_flag(txn, TXN_SUPPORTS_MVCC));
	if (!stailq_empty(&txn->stmts)) {
		/*
		 * Если транзакция пишущая, абортим её, потому что она уже
		 * в любом случае не сможет быть закоммичена, после того как
         * однажды отправилась в read view.
		 */
		assert(txn->status == TXN_INPROGRESS);
		txn_abort_with_conflict(txn);
		return;
	}
	//for (int i = 0; i < MAX_TX_ENGINE_COUNT; i++) {
	//	struct engine *engine = engines[i];
	//	if (engine->flags & ENGINE_SUPPORTS_MVCC) {
	//		/*
	//		 * We must create a read view in all engines that
	//		 * support MVCC, even those that haven't been used in
	//		 * this transaction so far, because they may be enabled
	//		 * in future statements. To do that, we need to start
	//		 * this transaction in those engines first.
	//		 */
	//		VERIFY(txn_begin_in_engine(engine, txn) == 0);
	
    /*
     * Для простоты считаем, что у нас всего один движок, и он
     * обязательно поддерживает MVCC. В engine.h объявлен один
     * глобальный engine.
     */
			memtx_engine_send_to_read_view(/*engine, */txn, psn);
    //	} else {
	//		assert(txn->engines[i] == NULL);
	//	}
	//}

	txn->status = TXN_IN_READ_VIEW;
}

void
txn_abort_with_conflict(struct txn *txn)
{
	if (txn->status == TXN_ABORTED)
		return;
	assert(txn->status == TXN_INPROGRESS || txn->status == TXN_IN_READ_VIEW);
	//assert(txn_has_flag(txn, TXN_SUPPORTS_MVCC));
	//for (int i = 0; i < MAX_TX_ENGINE_COUNT; i++) {
	//	struct engine *engine = txn->engines[i];
	//	if (engine != NULL)
			memtx_engine_abort_with_conflict(/*engine, */txn);
	//}
	txn->status = TXN_ABORTED;
	txn_set_flags(txn, TXN_IS_CONFLICTED);
}

static void
txn_rollback_one_stmt(struct txn *txn, struct txn_stmt *stmt)
{
	memtx_engine_rollback_statement(/*engine, */txn, stmt);
}

inline static struct txn *
txn_new(void)
{
	/* Place txn structure on the region. */
	int size;
	struct txn *txn = (struct txn *)malloc(sizeof(struct txn));
	if (txn == NULL) {
		fprintf(stderr, "Failed to allocate %u bytes in %s for %s", sizeof(struct txn), "malloc", "txn");
		return NULL;
	}
	rlist_create(&txn->read_set);
	rlist_create(&txn->point_holes_list);
	rlist_create(&txn->gap_list);
	rlist_create(&txn->in_read_view_txns);
	rlist_create(&txn->in_txns);
	return txn;
}

void
txn_free(struct txn *txn)
{
	memtx_tx_clean_txn(txn);
	struct txn_stmt *stmt;
	stailq_foreach_entry(stmt, &txn->stmts, next)
		txn_stmt_destroy(stmt);
	rlist_del(&txn->in_txns);
}

void
txn_rollback_stmt(struct txn *txn)
{
	if (txn == NULL || stailq_empty(&txn->stmts))
		return;
	struct txn_stmt *stmt = stailq_last(&txn->stmts);
	txn_rollback_one_stmt(txn, stmt);
	txn_stmt_destroy(stmt);
	stmt->space = NULL;
}

void
txn_rollback(struct txn *txn)
{
	assert(txn == in_txn());
	txn->status = TXN_ABORTED;
	assert(!txn_has_flag(txn, TXN_IS_DONE));
	assert(in_txn() == txn);
	txn->status = TXN_ABORTED;
	txn_set_flags(txn, TXN_IS_ROLLED_BACK);
	struct txn_stmt *stmt;
	stailq_reverse(&txn->stmts);
	stailq_foreach_entry(stmt, &txn->stmts, next)
		txn_rollback_one_stmt(txn, stmt);
	/* В случае mvcc это просто nop. */
    //engine_rollback(engine, txn);
	assert(txn->fiber == NULL);
    txn_free(txn);
	fiber_set_txn(fiber(), NULL);
}

struct txn *
txn_begin(void)
{
	static int64_t tsn = 0;
	assert(! in_txn());
	struct txn *txn = txn_new();
	if (txn == NULL)
		return NULL;

	rlist_add_tail_entry(&txns, txn, in_txns);
	stailq_create(&txn->stmts);
	txn->id = ++tsn;
	txn->psn = 0;
	txn->rv_psn = 0;
	txn->status = TXN_INPROGRESS;
	txn->fiber = NULL;
	fiber_set_txn(fiber(), txn);
	/* обновляет статы, нам не надо. */
    //memtx_tx_register_txn(txn);
	return txn;
}

int
txn_begin_stmt(struct txn *txn, struct memtx_space *space)
{
	assert(txn == in_txn());
	assert(txn != NULL);

	if (txn->status == TXN_IN_READ_VIEW)
		txn_abort_with_conflict(txn);

	if (txn_check_can_continue(txn) != 0)
		return -1;

	struct txn_stmt *stmt = txn_stmt_new(txn);
	if (stmt == NULL)
		return -1;

	stailq_add_tail_entry(&txn->stmts, stmt, next);

	if (space == NULL)
		return 0;

	stmt->space = space;
	return 0;
}

/** Prepare a transaction using engines, run triggers, etc. */
static int
txn_prepare(struct txn *txn)
{
	if (txn_check_can_continue(txn) != 0)
		return -1;

	assert(txn->psn == 0);
	/* psn должен быть выставлен до того, как позовутся engine-обработчки. */
	txn->psn = txn_next_psn++;

	memtx_engine_prepare(/*engine, */txn);
	memtx_tx_prepare_finalize(txn);

	txn->status = TXN_PREPARED;
	return 0;
}

int
txn_commit(struct txn *txn)
{
	txn->fiber = fiber();
	if (txn_prepare(txn) != 0)
		goto rollback;
	fiber_set_txn(fiber(), NULL);
	assert(!txn_has_flag(txn, TXN_IS_DONE));
	assert(in_txn() == txn);
	txn->status = TXN_COMMITTED;
	memtx_engine_commit(/*engine, */txn);
	assert(txn_has_flag(txn, TXN_IS_DONE));
	txn_free(txn);
	return 0;

rollback:
	assert(txn->fiber != NULL);
	if (!txn_has_flag(txn, TXN_IS_DONE)) {
		fiber_set_txn(fiber(), txn);
		txn_rollback(txn);
	} else {
		assert(in_txn() == NULL);
	}
	txn_free(txn);
	return -1;
}
