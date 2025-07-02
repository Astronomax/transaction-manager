#pragma once

#include "tuple.h"
#include "stdint.h"

/*
 * Для простоты у нас не будет составных ключей. Один ключ соответствует
 * одному полю. Соответственно key_def - это номер поля.
 */
typedef uint32_t key_def;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Сравнить тапл с ключом, используя key definition.
 * @param tuple tuple
 * @param key integer key
 * @param key_def key definition
 * @retval 0  if key_fields(tuple) == parts(key)
 * @retval <0 if key_fields(tuple) < parts(key)
 * @retval >0 if key_fields(tuple) > parts(key)
 */
int
tuple_compare_with_key(struct tuple *tuple, int key, key_def *key_def);

uint32_t
tuple_hash(struct tuple *tuple, key_def *key_def);

#ifdef __cplusplus
} // extern "C"
#endif
