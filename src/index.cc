#include "index.h"
#include "txn.h"
#include "stdbool.h"

int
index_check_dup(struct index *index, struct tuple *old_tuple, struct tuple *new_tuple, struct tuple *dup_tuple, enum dup_replace_mode mode)
{
    /*
     * old_tuple - это тот тапл, который должен быть удален. Бывают случаи,
     * когда у нас откуда-то в момент вызова уже есть информация о том, что
     * мы собираемся удалить, например, в случае DELETE.
     *
     * Если это REPLACE и index - primary, то old_tuple == NULL.
     * Если это REPLACE и index - secondary, то old_tuple == visible(directly_replaced[0]) (
     * то, что увидел REPLACE в момент замены по первичному ключю).
     * 
     * Если это DELETE, то old_tuple == index_get_internal(pk), который был вызван до того
     * как выполнить REPLACE(old_tuple, NULL).
     */
	if (dup_tuple == NULL) {
		if (mode == DUP_REPLACE) { //при REPLACE, по первичному ключу передается mode == DUP_REPLACE
			assert(old_tuple != NULL);
			/* При DUP_REPLACE обязательно должна произойти замена, а не вставка. */
			fprintf(stderr, "Attempt to modify a tuple field which is part of primary index in space %s", "TODO");
			goto fail;
		}
	} else { /* dup_tuple != NULL */
		if (dup_tuple != old_tuple && (old_tuple != NULL || mode == DUP_INSERT)) { //а по всем вторичным - DUP_INSERT
            /*
             * Поэтому все вторичные ключи должны удалять то же самое, что первичный.
			 * Мы не можем удалить более чем один тапл за раз.
			 */
			fprintf(stderr, "Duplicate key exists in unique index \"%s\" in space \"%s\" with old tuple - %s and new tuple - %s", "TODO", "TODO", "TODO", "TODO");
			goto fail;
		}
	}
	return 0;
fail:
	txn_set_flags(in_txn(), TXN_STMT_ROLLBACK);
	return -1;
}

int
index_get_internal(struct index *index, int key, struct tuple **result)
{
	return 0;
}

int
index_replace(struct index *index, struct tuple *old_tuple, struct tuple *new_tuple, enum dup_replace_mode mode, struct tuple **result/*, struct tuple **successor*/)
{

	return 0;
}

int
index_create(struct index *index)
{
	static uint32_t unique_id = 0;
	index->unique_id = unique_id++;
	/* Unusable until set to proper value during space creation. */
	index->dense_id = UINT32_MAX;
	rlist_create(&index->read_gaps);
}
