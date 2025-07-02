#pragma once

#include "index.h"
#include "tuple.h"
#include "txn.h"

struct memtx_space {
	uint32_t id;
	uint32_t index_count;
	struct index index[];
};

int
memtx_space_execute_replace(struct memtx_space *space, struct txn *txn, struct tuple *new_tuple, enum dup_replace_mode mode, struct tuple **result);

int
memtx_space_execute_delete(struct memtx_space *space, struct txn *txn, uint32_t index_id, int key, struct tuple **result);

struct memtx_space *
memtx_space_new(uint32_t index_count);
