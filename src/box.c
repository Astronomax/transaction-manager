#include "box.h"
#include "memtx_engine.h"
#include "memtx_engine.h"
#include "memtx_space.h"
#include "index.h"
#include "txn.h"

int
box_txn_begin(void)
{
	if (in_txn()) {
		fprintf(stderr, "Operation is not permitted when there is an active transaction");
		return -1;
	}
	if (txn_begin() == NULL)
		return -1;
	return 0;
}

int
box_txn_commit(void)
{
	struct txn *txn = in_txn();
	if (!txn)
		return 0;
	if (txn_check_can_complete(txn) != 0)
		return -1;
	return txn_commit(txn);
}

int
box_txn_rollback(void)
{
	struct txn *txn = in_txn();
	if (txn == NULL)
		return 0;
	if (txn_check_can_complete(txn) != 0)
		return -1;
	txn_rollback(txn); /* doesn't throw */
	return 0;
}

int
box_insert(struct memtx_space *space, struct tuple *new_tuple)
{
	struct tuple *tuple = NULL;
	struct txn *txn = in_txn();
    if (txn == NULL)
        return -1;
	if (txn_begin_stmt(txn, space) != 0)
        return -1;
	if (memtx_space_execute_replace(space, txn, new_tuple, DUP_INSERT, &tuple) != 0) {
		txn_rollback_stmt(txn);
		return -1;
	}
    return 0;
}

int
box_replace(struct memtx_space *space, struct tuple *new_tuple)
{
	struct tuple *tuple = NULL;
	struct txn *txn = in_txn();
    if (txn == NULL)
        return -1;
	if (txn_begin_stmt(txn, space) != 0)
        return -1;
	if (memtx_space_execute_replace(space, txn, new_tuple, DUP_REPLACE_OR_INSERT, &tuple) != 0) {
		txn_rollback_stmt(txn);
		return -1;
	}
    return 0;
}

int
box_delete(struct memtx_space *space, uint32_t index_id, int key)
{
	struct tuple *tuple = NULL;
	struct txn *txn = in_txn();
    if (txn == NULL)
        return -1;
	if (txn_begin_stmt(txn, space) != 0)
        return -1;
	if (memtx_space_execute_delete(space, txn, index_id, key, &tuple) != 0) {
		txn_rollback_stmt(txn);
		return -1;
	}
    return 0;
}
