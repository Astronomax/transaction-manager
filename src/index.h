#pragma once

#include "key_def.h"
#include "small/rlist.h"
#include "stdint.h"

#ifdef __cplusplus
#include <unordered_map>
#endif

enum dup_replace_mode {
	DUP_REPLACE_OR_INSERT,
	DUP_INSERT,
	DUP_REPLACE
};

struct tuple;

//typedef struct index index;
struct index {
	/** Space id. */
	uint32_t space_id;
	/** Index key definition. */
	key_def _key_def;
	/** Globally unique ID. */
	uint32_t unique_id;
	/** Compact ID - index in space->index array. */
	uint32_t dense_id;
    /*
	 * Зафиксированные случаи, отсутствия ключа в момент выполнения REPLACE,
	 * элемент был самым правым в индексе в момент вставки.
	 */
	struct rlist read_gaps;
#ifdef __cplusplus
	std::unordered_map<int, tuple *> tree;
#endif
};

#ifdef __cplusplus
extern "C" {
#endif

int
index_check_dup(struct index *index, struct tuple *old_tuple, struct tuple *new_tuple, struct tuple *dup_tuple, enum dup_replace_mode mode);

int
index_get_internal(struct index *index, int key, struct tuple **result);

int
index_replace(struct index *index, struct tuple *old_tuple, struct tuple *new_tuple, enum dup_replace_mode mode, struct tuple **result/*, struct tuple **successor*/);

int
index_create(struct index *index);

#ifdef __cplusplus
} // extern "C"
#endif
