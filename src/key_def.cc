#include "key_def.h"
#include "tuple.h"

int
tuple_compare_with_key(struct tuple *tuple, int key, key_def *key_def)
{
	return tuple->data[*key_def] - key;
}

uint32_t
tuple_hash(struct tuple *tuple, key_def *key_def)
{
    return tuple->data[*key_def];
}
