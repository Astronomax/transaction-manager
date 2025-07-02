#pragma once

#include "small/rlist.h"
#include "index.h"
#include "tuple.h"
#include "memtx_space.h"
#include "txn.h"

/**
 * Status of story. Describes the reason why it is not deleted.
 * In the case when story fits several statuses at once, status with
 * least value is chosen.
 */
enum memtx_tx_story_status {
	/**
	 * The story is used directly by some transactions.
	 */
	MEMTX_TX_STORY_USED = 0,
	/**
	 *  The story can be used by a read view.
	 */
	MEMTX_TX_STORY_READ_VIEW = 1,
	/**
	 * The story is used for gap tracking.
	 */
	MEMTX_TX_STORY_TRACK_GAP = 2,
	MEMTX_TX_STORY_STATUS_MAX = 3,
};

/**
 * Initialize memtx transaction manager.
 */
void
memtx_tx_manager_init();

/**
 * Free resources of memtx transaction manager.
 */
void
memtx_tx_manager_free();

/**
 * Implementation of engine_send_to_read_view callback.
 * Do not use directly.
 */
void
memtx_tx_abort_with_conflict(struct txn *txn);

/**
 * Implementation of engine_abort_with_conflict callback.
 * Do not use directly.
 */
void
memtx_tx_send_to_read_view(struct txn *txn, int64_t psn);

/**
 * @brief Add a statement to transaction manager's history.
 * Until unlinking or releasing the space could internally contain
 * wrong tuples and must be cleaned through memtx_tx_tuple_clarify call.
 * With that clarifying the statement will be visible to current transaction,
 * but invisible to all others.
 * Follows signature of @sa memtx_space_replace_all_keys .
 *
 * NB: can trigger story garbage collection.
 *
 * @param stmt current statement.
 * @param old_tuple the tuple that should be removed (can be NULL).
 * @param new_tuple the tuple that should be inserted (can be NULL).
 * @param mode      dup_replace_mode, used only if new_tuple is not
 *                  NULL and old_tuple is NULL, and only for the
 *                  primary key.
 * @param result - old or replaced tuple.
 * @return 0 on success, -1 on error (diag is set).
 */
int
memtx_tx_history_add_stmt(struct txn_stmt *stmt, struct tuple *old_tuple, struct tuple *new_tuple, enum dup_replace_mode mode, struct tuple **result);

/**
 * @brief Rollback (undo) a statement from transaction manager's history.
 * It's just make the statement invisible to all.
 * Prepared statements could be also removed, but for consistency all latter
 * prepared statement must be also rolled back.
 *
 * NB: can trigger story garbage collection.
 *
 * @param stmt current statement.
 */
void
memtx_tx_history_rollback_stmt(struct txn_stmt *stmt);

/**
 * @brief Prepare statement in history for further commit.
 * Prepared statements are still invisible for read-only transactions
 * but are visible to all read-write transactions.
 * Prepared and in-progress transactions use the same links for creating
 * chains of stories in history. The difference is that the order of
 * prepared transactions is fixed while in-progress transactions are
 * added to the end of list in any order. Thus to switch to prepared
 * we have to reorder story in such a way that current story will be
 * between earlier prepared stories and in-progress stories. That's what
 * this function does.
 *
 * NB: can trigger story garbage collection.
 *
 * @param stmt current statement.
 */
void
memtx_tx_history_prepare_stmt(struct txn_stmt *stmt);

/* Хелпер функции `memtx_tx_prepare_finalize`. */
void
memtx_tx_prepare_finalize_slow(struct txn *txn);

/**
 * Закончить preparing транзакции.
 * Должно быть вызвано для всей транзакции после того, как
 * `memtx_tx_history_rollback_stmt` была вызвана для каждого стейтмента в транзакции.
 *
 * NB: can trigger story garbage collection.
 */
static inline void
memtx_tx_prepare_finalize(struct txn *txn)
{
	memtx_tx_prepare_finalize_slow(txn);
}

/**
 * @brief Commit statement in history.
 * Make the statement's changes permanent. It becomes visible to all.
 *
 * NB: can trigger story garbage collection.
 *
 * @param stmt current statement.
 */
void
memtx_tx_history_commit_stmt(struct txn_stmt *stmt);

/** Хелпер функции memtx_tx_tuple_clarify */
struct tuple *
memtx_tx_tuple_clarify_slow(struct txn *txn, struct memtx_space *space, struct tuple *tuples, struct index *index/*, uint32_t mk_index*/);

/** Хелпер функции memtx_tx_track_point */
void
memtx_tx_track_point_slow(struct txn *txn, struct index *index, int key);

/**
 * Записать в TX менеджере, что транзакция @a txn ничего не прочитала
 * в спейсе @a space и индексе @a index по ключу @a key.
 *
 * Используется в ..._index_get_internal, остальные места использования
 * нас не интересуют. Вызывается, когда в индексе ничего не нашлось.
 * Если в индексе нашелся какой-то тапл, то там позовется memtx_tx_tuple_clarify,
 * который в свою очередь вызовет memtx_tx_track_story_gap, если никакой тапл,
 * кроме NULL, не видим для транзакции.
 * 
 * NB: can trigger story garbage collection.
 *
 * @return 0 on success, -1 on memory error.
 */
static inline void
memtx_tx_track_point(struct txn *txn, struct memtx_space *space, struct index *index, int key)
{
	//if (!memtx_tx_manager_use_mvcc_engine)
	//	return;
	if (txn == NULL || space == NULL/* || space->def->opts.is_ephemeral*/)
		return;
	memtx_tx_track_point_slow(txn, index, key);
}

/**
 * Clean a tuple if it's dirty - finds a visible tuple in history.
 *
 * @param txn - current transactions.
 * @param space - space in which the tuple was found.
 * @param tuple - tuple to clean.
 * @param index - index in which the tuple was found.
 * @param mk_index - multikey index (iа the index is multikey).
 * @return clean tuple (can be NULL).
 */
static inline struct tuple *
memtx_tx_tuple_clarify(struct txn *txn, struct memtx_space *space, struct tuple *tuple, struct index *index/*, uint32_t mk_index*/)
{
	//if (!memtx_tx_manager_use_mvcc_engine)
	//	return tuple;
	return memtx_tx_tuple_clarify_slow(txn, space, tuple, index/*, mk_index*/);
}
