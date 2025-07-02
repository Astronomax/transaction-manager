#pragma once

#include "memtx_space.h"
#include "fiber.h"
#include "salad/stailq.h"
#include "small/rlist.h"
#include "stdbool.h"

/**
 * Incremental counter for psn (prepare sequence number) of a transaction.
 * The next prepared transaction will get psn == txn_next_psn++.
 * See also struct txn::psn.
 */
extern int64_t txn_next_psn;

/** List of all in-progress transactions. */
extern struct rlist txns;

enum txn_flag {
	TXN_IS_DONE = 0x1,
	//TXN_IS_ABORTED_BY_YIELD = 0x2,
	TXN_CAN_YIELD = 0x4,
	//TXN_HAS_TRIGGERS = 0x8,
	//TXN_WAIT_SYNC = 0x10,
	//TXN_WAIT_ACK = 0x20,
	//TXN_FORCE_ASYNC = 0x40,
	TXN_IS_CONFLICTED = 0x80,
	//TXN_IS_ABORTED_BY_TIMEOUT = 0x100,
	TXN_IS_ROLLED_BACK = 0x200,
	//TXN_IS_STARTED_IN_ENGINE = 0x400,
	//TXN_SUPPORTS_MVCC = 0x800,
	TXN_STMT_ROLLBACK = 0x1000,
	//TXN_IS_ABORTED_RO_NODE = 0x2000,
};

enum {
	/**
	 * The minimal PSN (prepare sequence number) that can be assigned to a
	 * prepared transaction (see struct txn::psn). All values below this
	 * threshold can be used by a transaction manager as values with
	 * special meaning that no real transaction can have.
	 */
	TXN_MIN_PSN = 2,
};

enum txn_status {
	TXN_INPROGRESS,
	TXN_PREPARED,
	TXN_IN_READ_VIEW,
	TXN_COMMITTED,
	TXN_ABORTED,
};

/* Уровни изоляции, как ни странно, сейчас нас не сильно интересуют. */
enum txn_isolation_level {
	TXN_ISOLATION_DEFAULT,
	TXN_ISOLATION_READ_COMMITTED,
	TXN_ISOLATION_READ_CONFIRMED,
	TXN_ISOLATION_BEST_EFFORT,
	TXN_ISOLATION_LINEARIZABLE,
	txn_isolation_level_MAX,
};

struct txn;

/**
 * Structure which contains pointers to the tuples,
 * that are used in rollback. Currently used only in
 * memtx engine.
 */
struct txn_stmt_rollback_info {
	struct tuple *old_tuple;
	struct tuple *new_tuple;
};

struct txn_stmt {
	/** A linked list of all statements. */
	struct stailq_entry next;
	struct txn *txn;
	struct memtx_space *space;
	struct tuple *old_tuple;
	struct tuple *new_tuple;
	struct txn_stmt_rollback_info rollback_info;
    /* story, которая была введена данным стейтментом. */
	struct memtx_story *add_story;
    /* story, действие которой было прекращено данным стейтментом. */
	struct memtx_story *del_story;
    /* одна story может быть удалена несколькими стейтментами. */
	struct txn_stmt *next_in_del_list;
    /*
     * Флаг, показывающий, перезаписывает ли этот стейтмент другой собственный
     * стейтмент соответствующей транзакции. Например, если транзакция делает
     * два реплейса одного и того же ключа, второй оператор будет с
     * is_own_change = true. Или если транзакция удаляет какой-то ключ, а затем
     * вставляет этот ключ, оператор вставки будет с is_own_change = true.
     */
	bool is_own_change;
};

struct txn {
	int64_t id;
	int64_t psn;
	int64_t rv_psn;
	enum txn_status status;
	//enum txn_isolation_level isolation;
	struct stailq stmts;
    unsigned flags;
	struct fiber *fiber;
	struct rlist in_read_view_txns;
	struct rlist read_set;
	struct rlist point_holes_list;
	struct rlist gap_list;
	struct rlist in_txns;
};

static inline bool
txn_has_flag(const struct txn *txn, enum txn_flag flag)
{
	assert((flag & (flag - 1)) == 0);
	return (txn->flags & flag) != 0;
}

static inline void
txn_set_flags(struct txn *txn, unsigned int flags)
{
	txn->flags |= flags;
}

/**
 * Returns the code of the error that caused abort of the given transaction.
 */
static inline /*enum box_error_code*/ char *
txn_flags_to_error_message(struct txn *txn)
{
	//if (txn_has_flag(txn, TXN_IS_ABORTED_RO_NODE))
	//	return ER_READONLY;
	//if (txn_has_flag(txn, TXN_IS_CONFLICTED)) {
	//	fprintf(stderr, "abort0\n");
	//	fflush(stderr);
	//	return ER_TRANSACTION_CONFLICT;
	//}
	//else if (txn_has_flag(txn, TXN_IS_ABORTED_BY_YIELD))
	//	return ER_TRANSACTION_YIELD;
	//else if (txn_has_flag(txn, TXN_IS_ABORTED_BY_TIMEOUT))
	//	return ER_TRANSACTION_TIMEOUT;
	//else if (txn_has_flag(txn, TXN_IS_ROLLED_BACK))
	//	return ER_TXN_ROLLBACK;
	//return ER_UNKNOWN;
	return "TODO";
}

/**
 * Checks if new statements can be executed in the given transaction.
 * Returns 0 if true. Otherwise, sets diag and returns -1.
 */
static inline int
txn_check_can_continue(struct txn *txn)
{
	enum txn_status status = txn->status;
	if (status == TXN_ABORTED) {
		fprintf(stderr, txn_flags_to_error_message(txn));
		return -1;
	} else if (status == TXN_COMMITTED) {
		fprintf(stderr, "Transaction was committed");
		return -1;
	}
	return 0;
}

/**
 * Checks if the transaction can be completed (rollback or commit).
 * There are cases when transaction cannot be continued (txn_check_can_continue
 * will return error) but it's allowed to try to commit or rollback it.
 * Example: when MVCC aborts a transaction due to conflict, the result is
 * not observed by user. So he cannot execute new statements in the transaction
 * but we cannot forbid to try to commit the transaction - only then the error
 * will be observed and the transaction will be actually completed (rolled back
 * due to conflict). But after the attempt to commit the transaction, we must
 * forbid to try to complete it again - it will lead to UB.
 * Returns 0 if true. Otherwise, sets diag and returns -1.
 */
static inline int
txn_check_can_complete(struct txn *txn)
{
	enum txn_status status = txn->status;
	if (status == TXN_ABORTED && txn_has_flag(txn, TXN_IS_ROLLED_BACK)) {
		/* Cannot complete already rolled back transaction. */
		fprintf(stderr, "Transaction was rolled back");
		return -1;
	} else if (status == TXN_COMMITTED) {
		/* Cannot complete already committed transaction. */
		fprintf(stderr, "Transaction was committed");
		return -1;
	}
	return 0;
}

/* Pointer to the current transaction (if any) */
static inline struct txn *
in_txn(void)
{
	return fiber()->txn;
}

/* Set to the current transaction (if any) */
static inline void
fiber_set_txn(struct fiber *fiber, struct txn *txn)
{
	fiber->txn = txn;
}

/** The current statement of the transaction. */
static inline struct txn_stmt *
txn_current_stmt(struct txn *txn)
{
	return stailq_last_entry(&txn->stmts, struct txn_stmt, next);
}

struct txn *
txn_begin(void);

int
txn_begin_stmt(struct txn *txn, struct memtx_space *space);

int
txn_commit(struct txn *txn);

void
txn_rollback_stmt(struct txn *txn);

void
txn_rollback(struct txn *txn);

/**
 * If the given transaction is read-only, send it to a read view in which it
 * can't see changes done with the given PSN or newer, otherwise abort it as
 * conflicted immediately.
 */
void
txn_send_to_read_view(struct txn *txn, int64_t psn);

/**
 * Mark a transaction as conflicted and abort it.
 * Does nothing if the transaction is already aborted.
 */
void
txn_abort_with_conflict(struct txn *txn);
