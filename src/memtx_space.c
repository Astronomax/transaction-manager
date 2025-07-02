#include "memtx_space.h"
#include "assert.h"

int
memtx_space_replace/*_all_keys*/(struct memtx_space *space, struct tuple *old_tuple, struct tuple *new_tuple, enum dup_replace_mode mode, struct tuple **result)
{
	assert(space->index_count > 0);
	struct index *pk = &space->index[0];
	/* Для простоты у нас все индексы будут уникальными. */
	//assert(pk->def->opts.is_unique);

	/* Реплейсы должны происходить внутри транзакции. */
	assert(/*space->def->opts.is_ephemeral ||*/
	       (in_txn() != NULL && txn_current_stmt(in_txn()) != NULL));
	/* ephemeral поддерживать не будем. */
	//if (memtx_tx_manager_use_mvcc_engine && !space->def->opts.is_ephemeral) {
		struct txn_stmt *stmt = txn_current_stmt(in_txn());
		return memtx_tx_history_add_stmt(stmt, old_tuple, new_tuple, mode, result);
	//}
}

static inline int
memtx_space_replace_tuple(struct memtx_space *space, struct txn_stmt *stmt, struct tuple *old_tuple, struct tuple *new_tuple, enum dup_replace_mode mode)
{
	struct memtx_space *memtx_space = (struct memtx_space *)space;
	struct tuple *result;
	int rc = memtx_space_replace(space, old_tuple, new_tuple, mode, &result);
	if (rc != 0)
		return rc;
	txn_stmt_prepare_rollback_info(stmt, result, new_tuple);
	stmt->new_tuple = new_tuple;
	stmt->old_tuple = result;
	return 0;
}

int
memtx_space_execute_replace(struct memtx_space *space, struct txn *txn, struct tuple *new_tuple, enum dup_replace_mode mode, struct tuple **result)
{
	struct txn_stmt *stmt = txn_current_stmt(txn);
	if (new_tuple == NULL) {
		return -1;
	}
	if (memtx_space_replace_tuple(space, stmt, NULL, new_tuple, mode) != 0)
		return -1;
	*result = stmt->new_tuple;
	return 0;
}

int
memtx_space_execute_delete(struct memtx_space *space, struct txn *txn, uint32_t index_id, int key, struct tuple **result)
{
	struct txn_stmt *stmt = txn_current_stmt(txn);
	/* Try to find the tuple by unique key. */
	assert(index_id < space->index_count);
	struct index *pk = &space->index[index_id];//index_find(space, index_id);
	if (pk == NULL)
		return -1;
	struct tuple *old_tuple;
	if (index_get_internal(pk, key, &old_tuple) != 0)
		return -1;
	if (old_tuple == NULL) {
		*result = NULL;
		return 0;
	}
	if (memtx_space_replace_tuple(space, stmt, old_tuple, NULL, DUP_REPLACE_OR_INSERT) != 0)
		return -1;
	*result = stmt->old_tuple;
	return 0;
}

struct memtx_space *
memtx_space_new(uint32_t index_count)
{
	struct memtx_space *memtx_space = malloc(sizeof(struct memtx_space) + sizeof(struct index) * index_count);
	if (memtx_space == NULL) {
		fprintf(stderr, "Failed to allocate %u bytes in %s for %s", sizeof(struct memtx_space), "malloc", "struct memtx_space");
		return NULL;
	}
	static uint32_t space_id = 0;
	memtx_space->id = space_id++;
	for (int i = 0; i < index_count; i++) {
		index_create(&memtx_space->index[i]);
		memtx_space->index[i].space_id = memtx_space->id;
		memtx_space->index[i]._key_def = i;
		memtx_space->index[i].dense_id = i;
	}
	memtx_space->index_count = index_count;
	return memtx_space;
}
