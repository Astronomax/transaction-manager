#pragma once

#include "assert.h"
#include "stdbool.h"
#include "stdint.h"
#ifdef __cplusplus
#include <vector>
#endif

enum tuple_flag {
	//TUPLE_HAS_UPLOADED_REFS = 0,
	TUPLE_IS_DIRTY = 1,
	//TUPLE_IS_TEMPORARY = 2,
	tuple_flag_MAX,
};

//typedef struct tuple tuple;
struct tuple {
    uint8_t flags;
#ifdef __cplusplus
    std::vector<int> data;
#endif
};

/** Set flag of the tuple. */
static inline void
tuple_set_flag(struct tuple *tuple, enum tuple_flag flag)
{
	assert(flag < tuple_flag_MAX);
	tuple->flags |= (1 << flag);
}

/** Test if tuple has a flag. */
static inline bool
tuple_has_flag(struct tuple *tuple, enum tuple_flag flag)
{
	assert(flag < tuple_flag_MAX);
	return (tuple->flags & (1 << flag)) != 0;
}

/** Clears tuple flag. */
static inline void
tuple_clear_flag(struct tuple *tuple, enum tuple_flag flag)
{
	assert(flag < tuple_flag_MAX);
	tuple->flags &= ~(1 << flag);
}

#ifdef __cplusplus
extern "C" {
#endif

char *
tuple_str(struct tuple *tuple);

#ifdef __cplusplus
} // extern "C"
#endif
