#include "memtx_tx.h"
#include "key_def.h"
#include "salad/stailq.h"
#include "memtx_space.h"

enum {
	/**
	 * Фиктивный PSN, который выставляется в del_psn откаченной story.
	 * Должен быть меньше, чем существующие "реальные" PSN.
	 */
	MEMTX_TX_ROLLBACKED_PSN = 1,
};

static_assert((int)MEMTX_TX_ROLLBACKED_PSN < (int)TXN_MIN_PSN,
	      "There must be a range for TX manager's internal use");

struct memtx_story;

struct memtx_story_link {
	struct memtx_story *newer_story;
	struct memtx_story *older_story;
    /*
	 * Зафиксированные случаи, отсутствия ключа в момент выполнения REPLACE,
	 * тапл, соответствующий данной стори, был следующим справа от вставляемого тапла.
	 */
	struct rlist read_gaps;
	/*
	 * != NULL, если и только если соотв. story представлена в индексе (соотв.
	 * тапл физически лежит в дереве индекса). Указывает, очевидно, на индекс,
	 * в котором тапл находится.
	 */
	struct index *in_index;
};

struct memtx_story {
	struct tuple *tuple;
    /* Стейтмент, создавший данную story. Является NULL, если соответствующая
     * транзакция была закоммичена, либо произошла очень давно, и мы даже не знаем,
     * какой транзакцией, данная story была создана.
     */
	struct txn_stmt *add_stmt;
    /*
     * PSN транзакции, соответствующей add_stmt. Является 0, если транзакция
     * еще не была prepared (является is progress), либо если add_stmt == NULL;
     */
	int64_t add_psn;
    /*
     * Стейтменты, которые удалили данный тапл (прекратили действие данной story).
     * Выставляется в NULL, когда стейтмент оказывается закоммиченным.
     * Так же может быть NULL, если тапл еще не был удален никаким стейтментом.
     */
	struct txn_stmt *del_stmt;
    /*
     * PSN транзакции, соответствующей del_stmt. Является 0, если транзакция
     * еще не была prepared (является is progress), либо если del_stmt == NULL;
     */
	int64_t del_psn;
    /* Список трекеров - транзакции, которые прочитали данный тапл.*/
	struct rlist reader_list;
	/* Link in tx_manager::all_stories */
	struct rlist in_all_stories;
	/* Кол-во индексов мы тут прихранили. */
	uint32_t index_count;
	enum memtx_tx_story_status status;
	struct memtx_story_link link[];
};

static uint32_t
memtx_tx_story_key_hash(const struct tuple *a)
{
	uintptr_t u = (uintptr_t)a;
	if (sizeof(uintptr_t) <= sizeof(uint32_t))
		return u;
	else
		return u ^ (u >> 32);
}

#define mh_name _history
#define mh_key_t struct tuple *
#define mh_node_t struct memtx_story *
#define mh_arg_t int
#define mh_hash(a, arg) (memtx_tx_story_key_hash((*(a))->tuple))
#define mh_hash_key(a, arg) (memtx_tx_story_key_hash(a))
#define mh_cmp(a, b, arg) ((*(a))->tuple != (*(b))->tuple)
#define mh_cmp_key(a, b, arg) ((a) != (*(b))->tuple)
#define MH_SOURCE
#include "salad/mhash.h"

/* В первых вериях это называлось просто tx_conflict_tracker */
struct tx_read_tracker {
	struct txn *reader;
	struct memtx_story *story;
	struct rlist in_reader_list;
	struct rlist in_read_set;
};

/**
 * Элемент, который содержит информацию о том, что какая-то транзакция
 * прочитала full key и ничего не нашли.
 */
struct point_hole_item {
	/*
	 * Ссылка в зацикленном списке элементов с таким же индексом и ключом.
	 * Нам не интересно, мы будем хранить элементы в векторе.
	 */
	struct rlist ring;
	/** Ссылка в txn->point_holes_list. */
	struct rlist in_point_holes_list;
	/** Saved index->unique_id. */
	uint32_t index_unique_id;
	/** Precalculated hash for storing in hash table. */
	uint32_t hash;
	struct txn *txn;
	int key;
	/** Flag that the hash tables stores pointer to this item. */
	bool is_head;
};

struct inplace_gap_item {
    /* Якорь в memtx_story_link::read_gaps или index::read_gaps. */
	struct rlist in_read_gaps;
	struct rlist in_gap_list;
	struct txn *txn;
};

static void
gap_item_base_create(struct inplace_gap_item *item, struct txn *txn) {
	item->txn = txn;
    /* У транзакции может быть несколько inplace_gap_item. */
	rlist_add(&txn->gap_list, &item->in_gap_list);
}

/** Учтите, что in_read_gaps должен быть проинициализирован позже. */
static struct inplace_gap_item *
memtx_tx_inplace_gap_item_new(struct txn *txn) {
	struct inplace_gap_item *item = (struct inplace_gap_item *)malloc(sizeof(struct inplace_gap_item));
	gap_item_base_create(item, txn);
	return item;
}

static void
memtx_tx_inplace_gap_item_delete(struct inplace_gap_item *item) {
    /* Удаляем из обоих списков. */
    rlist_del(&item->in_gap_list);
	rlist_del(&item->in_read_gaps);
    free(item);
}

/* Хелпер структура для поиска point_hole_item в хеш-таблице */
struct point_hole_key {
	/** Индекс, в котором осуществлялся поиск. */
	struct index *index;
	/** Мы используем тапл как ключ - он сравнивается с ключом из point_hole_item. */
	struct tuple *tuple;
	/* func_key пока игнорируем. */
	/** Functional key of the tuple, must be set if index is functional. */
	//tuple *func_key;
};

static uint32_t
point_hole_storage_combine_index_and_tuple_hash(struct index *index, uint32_t tuple_hash)
{
	return (uintptr_t)index ^ tuple_hash;
}

/** Хеш ключа. */
static uint32_t
point_hole_storage_key_hash(struct point_hole_key *key)
{
	key_def *def = &key->index->_key_def;
	//uint32_t tuple_hash = 0;
	/* func_key пока игнорируем. */
	//if (likely(!def->for_func_index)) {
	//	assert(key->func_key == NULL);
	//	tuple_hash = tuple_hash(key->tuple, def);
	//} else {
	//	assert(key->func_key != NULL);
	//	const char *data = tuple_data(key->func_key);
	//	mp_decode_array(&data);
	//	tuple_hash = key_hash(data, def);
	//}
	return point_hole_storage_combine_index_and_tuple_hash(key->index, tuple_hash(key->tuple, def));
}

/** point_hole_item компаратор. */
static int
point_hole_storage_equal(const struct point_hole_item *obj1, const struct point_hole_item *obj2)
{
	/* Canonical msgpack is comparable by memcmp. */
	if (obj1->index_unique_id != obj2->index_unique_id)
		return 1;
	return obj1->key == obj2->key;
}

/** point_hole_item компаратор с ключом. */
static int
point_hole_storage_key_equal(const struct point_hole_key *key, const struct point_hole_item *object)
{
	if (key->index->unique_id != object->index_unique_id)
		return 1;
	assert(key->index != NULL);
	assert(key->tuple != NULL);
	key_def *def = &key->index->_key_def;
	/* Для простоты считаем, что никаких хинтов у нас пока нет. */
	//uint64_t tuple_hint = HINT_NONE;
	/* func_key пока игнорируем. */
	//if (unlikely(def->for_func_index))
	//	tuple_hint = (uint64_t)key->func_key;
	/*
	 * Note that it's OK to always pass HINT_NONE for the key - hints
	 * won't be used then if the index is not functional.
	 */
	return tuple_compare_with_key(key->tuple, object->key, def);
}

#define mh_name _point_holes
#define mh_key_t struct point_hole_key *
#define mh_node_t struct point_hole_item *
#define mh_arg_t int
#define mh_hash(a, arg) ((*(a))->hash)
#define mh_hash_key(a, arg) ( point_hole_storage_key_hash(a) )
#define mh_cmp(a, b, arg) point_hole_storage_equal(*(a), *(b))
#define mh_cmp_key(a, b, arg) point_hole_storage_key_equal((a), *(b))
#define MH_SOURCE
#include "salad/mhash.h"

struct tx_manager
{
    /*
     * Список всех read-only транзакций, отправленных в read view.
     * Новые транзакции добавляются в конец списка. Список всегда
     * отсортирован по rv_psn.
     */
	struct rlist read_view_txns;
    /* Маппинг из таплов в соответствующие story. */
    struct mh_history_t *history;
	/*
	 * Хеш таблица, которая предназначена для того, чтобы хранить ситуации, когда
	 * транзакция ничего не прочитала в определенном месте, при цепочка по данному
	 * ключу пока пустая и негде сохранить эту инфу, поэтому придумали хранить в мапчике
	 * до момента, пока не появлятся первый элемент в соотв. цепочке, в этот момент
	 * все элементы из соотв. списка в мапчике переносятся в список в story->link[i].read_gaps.
	 * 
	 * В первой версии этого не было. Не понятно, как без него обходились.
	 */
	struct mh_point_holes_t *point_holes;
	/** List of all memtx_story objects. */
	struct rlist all_stories;
	/** Iterator that sequentially traverses all memtx_story objects. */
	struct rlist *traverse_all_stories;
	/** Accumulated number of GC steps that should be done. */
	size_t must_do_gc_steps;
};

enum {
	/**
	 * Number of iterations that is allowed for TX manager to do for
	 * searching and deleting no more used memtx_tx_stories per creation of
	 * a new story.
	 */
	TX_MANAGER_GC_STEPS_SIZE = 2,
};

/* Менеджер */
static struct tx_manager txm;

/* Очистить все read списки транзакции @a txn. */
static void
memtx_tx_clear_txn_read_lists(struct txn *txn);

/**
 * Вставляет (или перемещает, если транзакция уже находится в read view)
 * транзакцию в read view так, чтобы read view оставался отсортированным по rv_psn.
 */
static void
memtx_tx_adjust_position_in_read_view_list(struct txn *txn)
{
	if (txn->in_read_view_txns.prev == &txm.read_view_txns)
		return;
	struct txn *prev_txn = rlist_prev_entry(txn, in_read_view_txns);
	if (prev_txn->rv_psn <= txn->rv_psn)
		return;
	rlist_del(&txn->in_read_view_txns);
	while (prev_txn->in_read_view_txns.prev != &txm.read_view_txns) {
		struct txn *scan = rlist_prev_entry(prev_txn, in_read_view_txns);
		if (scan->rv_psn <= txn->rv_psn)
			break;
		prev_txn = scan;
	}
	rlist_add_tail(&prev_txn->in_read_view_txns, &txn->in_read_view_txns);
}

void
memtx_tx_abort_with_conflict(struct txn *txn)
{
    /* Удалить из read view, если находимся в нем. */
	if (txn->status == TXN_IN_READ_VIEW)
		rlist_del(&txn->in_read_view_txns);
}

/**
 * Отправляем транзацию в read view, где должны находится только
 * read only транзакции.
 */
void
memtx_tx_send_to_read_view(struct txn *txn, int64_t psn)
{
	assert((txn->status == TXN_IN_READ_VIEW) == (txn->rv_psn != 0));
	if (txn->status != TXN_IN_READ_VIEW) {
		txn->rv_psn = psn;
		rlist_add_tail(&txm.read_view_txns, &txn->in_read_view_txns);
	} else if (txn->rv_psn > psn) {
		/*
		 * Обратите внимание, что в каждом случае для каждого ключа мы можем
         * выбрать любой psn read view между confirmed уровнем и самой
         * старой prepared транзакцией, которая изменяет этот ключ. 
         * Но мы выбираем последний уровень, потому что он обычно стоит дешевле,
         * и если есть несколько breakers - мы должны последовательно
         * уменьшать уровень read view.
		 */
		txn->rv_psn = psn;
	}
    /*
     * Либо транзакция только что была добавлена в конец, либо у нее обновился (уменьшился)
     * rv_psn. В любом случае может потребоваться переместить её на несколько позиций назад
     * в read view списке, чтобы сохранить его отсортированность.
     */
	memtx_tx_adjust_position_in_read_view_list(txn); // единственное место вызова
}

static inline void
memtx_tx_story_set_status(struct memtx_story *story, enum memtx_tx_story_status new_status)
{
	assert(story->status < MEMTX_TX_STORY_STATUS_MAX);
	story->status = new_status;
    //дальше обновление статистик - нам не интересно
}

static struct memtx_story *
memtx_tx_story_new(struct memtx_space *space, struct tuple *tuple)
{
	txm.must_do_gc_steps += TX_MANAGER_GC_STEPS_SIZE;
	assert(!tuple_has_flag(tuple, TUPLE_IS_DIRTY));
	uint32_t index_count = space->index_count;
	struct memtx_story *story = (struct memtx_story *)
        (malloc(sizeof(struct memtx_story) +
            index_count * sizeof(struct memtx_story_link)));
	story->tuple = tuple;

	const struct memtx_story **put_story =
	(const struct memtx_story **) &story;
	struct memtx_story *replaced = NULL;
	struct memtx_story **preplaced = &replaced;
	mh_history_put(txm.history, put_story, &preplaced, 0);
	assert(preplaced == NULL);
	story->status = MEMTX_TX_STORY_USED;

	story->index_count = index_count;
	story->add_stmt = NULL;
	story->add_psn = 0;
	story->del_stmt = NULL;
	story->del_psn = 0;
	rlist_create(&story->reader_list);
	rlist_add_tail(&txm.all_stories, &story->in_all_stories);

	for (uint32_t i = 0; i < index_count; i++) {
		story->link[i].newer_story = story->link[i].older_story = NULL;
		rlist_create(&story->link[i].read_gaps);
		story->link[i].in_index = &space->index[i];
	}
	return story;
}

/** Ожидается, что story полностью отсоединена. */
static void
memtx_tx_story_delete(struct memtx_story *story)
{
    /* Проверяем, что, действительно, отсоединена. */
	assert(story->add_stmt == NULL);
	assert(story->del_stmt == NULL);
	assert(rlist_empty(&story->reader_list));
	for (uint32_t i = 0; i < story->index_count; i++) {
		assert(story->link[i].newer_story == NULL);
		assert(story->link[i].older_story == NULL);
		assert(rlist_empty(&story->link[i].read_gaps));
	}

	if (txm.traverse_all_stories == &story->in_all_stories)
		txm.traverse_all_stories = rlist_next(txm.traverse_all_stories);
	rlist_del(&story->in_all_stories);

    /* Удаляем из мапчика tuple -> story. */
	mh_int_t pos = mh_history_find(txm.history, story->tuple, 0);
	assert(pos != mh_end(txm.history));
	mh_history_del(txm.history, pos, 0);

	tuple_clear_flag(story->tuple, TUPLE_IS_DIRTY);

	free(story);
}

static struct memtx_story *
memtx_tx_story_get(struct tuple *tuple) {
	assert(tuple_has_flag(tuple, TUPLE_IS_DIRTY));

	mh_int_t pos = mh_history_find(txm.history, tuple, 0);
	assert(pos != mh_end(txm.history));
	struct memtx_story *story = *mh_history_node(txm.history, pos);
	if (story->add_stmt != NULL)
		assert(story->add_psn == story->add_stmt->txn->psn);
	if (story->del_stmt != NULL)
		assert(story->del_psn == story->del_stmt->txn->psn);
	return story;
}

static void
memtx_tx_story_link_added_by(struct memtx_story *story, struct txn_stmt *stmt)
{
	assert(story->add_stmt == NULL);
	assert(stmt->add_story == NULL);
	story->add_stmt = stmt;
	stmt->add_story = story;
}

static void
memtx_tx_story_unlink_added_by(struct memtx_story *story, struct txn_stmt *stmt)
{
	assert(stmt->add_story == story);
	assert(story->add_stmt == stmt);
	stmt->add_story = NULL;
	story->add_stmt = NULL;
}

static void
memtx_tx_story_link_deleted_by(struct memtx_story *story, struct txn_stmt *stmt)
{
	assert(stmt->del_story == NULL);
	assert(stmt->next_in_del_list == NULL);

	stmt->del_story = story;
	stmt->next_in_del_list = story->del_stmt;
	story->del_stmt = stmt;
}

static void
memtx_tx_story_unlink_deleted_by(struct memtx_story *story, struct txn_stmt *stmt)
{
	assert(stmt->del_story == story);

	/* Find a place in list from which stmt must be deleted. */
    /* Пока не понятно, почему бы не использовать rlist здесь. */
	struct txn_stmt **ptr = &story->del_stmt;
	while (*ptr != stmt) {
		ptr = &(*ptr)->next_in_del_list;
		assert(ptr != NULL);
	}
	*ptr = stmt->next_in_del_list;
	stmt->next_in_del_list = NULL;
	stmt->del_story = NULL;
}

/**
 * Соединяет @a story с @a old_story в @a индексе (в обоих направлениях).
 * @a old_story может быть NULL.
 */
static void
memtx_tx_story_link(struct memtx_story *story, struct memtx_story *old_story, uint32_t idx)
{
	assert(idx < story->index_count);
	struct memtx_story_link *link = &story->link[idx];
	assert(link->older_story == NULL);

	if (old_story == NULL)
		return;

	assert(idx < old_story->index_count);
	struct memtx_story_link *old_link = &old_story->link[idx];
	assert(old_link->newer_story == NULL);

	link->older_story = old_story;
	old_link->newer_story = story;
}

/**
 * Отсоединяет @a story от @a old_story в @a индексе (в обоих направлениях).
 * Older story может быть NULL.
 */
static void
memtx_tx_story_unlink(struct memtx_story *story, struct memtx_story *old_story, uint32_t idx)
{
	assert(idx < story->index_count);
	struct memtx_story_link *link = &story->link[idx];
	assert(link->older_story == old_story);

	if (old_story == NULL)
		return;

	assert(idx < old_story->index_count);
	struct memtx_story_link *old_link = &old_story->link[idx];
	assert(old_link->newer_story == story);

	link->older_story = NULL;
	old_link->newer_story = NULL;
}

/**
 * Соединили @a new_top с @a old_top в @a idx (в обоих направлениях), где
 * @a old_top был на верхушке цепочки.
 * Есть две различных, но очень похожих реализационных сценариев, в которых
 * данная функция может использоваться:
 *
 * * @a is_new_tuple == true:
 *   @a new_top это только что созданная story для нового тапла, который
 *   только что был вставлен в индекс. @a old_top это story, которая до этого
 *   была на верхушке цепочки или NULL если цепочка была пустой.
 *
 * * @a is_new_tuple == false:
 *   @a old_top был на верхушке цепочки в то время как @a new_top был следующей
 *   story, и цепочка должна быть reordered и @a new_top должен попасть на
 *   верхушку цепочки и @a old_top должен быть соединен после него. Случай также
 *   требует физического replacement в индексе - он будет указывать на new_top->tuple.
 * 
 *	C is_new_tuple == false вызывается только из memtx_tx_story_reorder в момент,
 *	когда story свапается с верхушкой и становится новой верхушкой. Производим
 *	replacement, чтобы сохранить инвариант, что верхушка всегда находится в индексе.
 */
static void
memtx_tx_story_link_top(struct memtx_story *new_top, struct memtx_story *old_top, uint32_t idx, bool is_new_tuple)
{
	assert(old_top != NULL || is_new_tuple);
	if (is_new_tuple && old_top == NULL) {
		//if (idx == 0)
		//	memtx_tx_ref_to_primary(new_top);
		return;
	}
	struct memtx_story_link *new_link = &new_top->link[idx];
	struct memtx_story_link *old_link = &old_top->link[idx];
	assert(old_link->in_index != NULL);
	assert(old_link->newer_story == NULL);
	if (is_new_tuple) {
		assert(new_link->newer_story == NULL);
		assert(new_link->older_story == NULL);
	} else {
		assert(new_link->newer_story == old_top);
		assert(old_link->older_story == new_top);
	}

	if (!is_new_tuple) {
		/* Делаем физиески реплейс. */
		struct index *index = old_link->in_index;
		struct tuple *removed/*, *unused*/;
		if (index_replace(index, old_top->tuple, new_top->tuple, DUP_REPLACE, &removed/*, &unused*/) != 0) {
			/*panic*/fprintf(stderr, "failed to rebind story in index");
			exit(1);
		}
		assert(old_top->tuple == removed);
	}

	/* Link the list. */
	if (is_new_tuple) {
		memtx_tx_story_link(new_top, old_top, idx);
		/* in_index must be set in story_new. */
		assert(new_link->in_index == old_link->in_index);
		old_link->in_index = NULL;
	} else {
        /**
         * Свап old_top и new_top
         * older_story -> new_top -> old_top =>
         *      older_story -> old_top -> new_top
         */
		struct memtx_story *older_story = new_link->older_story;
		memtx_tx_story_unlink(old_top, new_top, idx);
		memtx_tx_story_unlink(new_top, older_story, idx);
		memtx_tx_story_link(new_top, old_top, idx);
		memtx_tx_story_link(old_top, older_story, idx);
		new_link->in_index = old_link->in_index;
		old_link->in_index = NULL;
	}

	/*
	 * Все таплы, которые физически находятся в первичном индексе должны
     * быть referenced. Нам это сейчас не очень важно.
	 */
	//if (idx == 0) {
	//	memtx_tx_ref_to_primary(new_top);
	//	memtx_tx_unref_from_primary(old_top);
	//}

	/*
     * Переносим все gap records в новую вершину списка.
     * Таким образом, в любой момент времени все пробелы (gap)
     * находятся в вершине, во всех предыдущих story списки пустые.
     */
	rlist_splice(&new_link->read_gaps, &old_link->read_gaps);
}

/** Свап двух соседних story. */
static void
memtx_tx_story_reorder(struct memtx_story *story, struct memtx_story *old_story, uint32_t idx)
{
	assert(idx < story->index_count);
	assert(idx < old_story->index_count);
	struct memtx_story_link *link = &story->link[idx];
	struct memtx_story_link *old_link = &old_story->link[idx];
	assert(link->older_story == old_story);
	assert(old_link->newer_story == story);
	struct memtx_story *newer_story = link->newer_story;
	struct memtx_story *older_story = old_link->older_story;

	/*
	 * older_story -> old_story -> story -> newer_story =>
     * older_story -> story -> old_story -> newer_story
	 */
	if (newer_story != NULL) {
		/* Это не верхушка списка, поэтому просто переприсоединяем все. */
		memtx_tx_story_unlink(newer_story, story, idx);
		memtx_tx_story_unlink(story, old_story, idx);
		memtx_tx_story_unlink(old_story, older_story, idx);

		memtx_tx_story_link(newer_story, old_story, idx);
		memtx_tx_story_link(old_story, story, idx);
		memtx_tx_story_link(story, older_story, idx);
	} else {
		/*
		 * Случай, когда свапаются две верхние story обрабатывается отдельно
         * в memtx_tx_story_link_top, чтобы учесть переезд пробелов из предыдущей
         * верхушки в новую, и сохранить тем самым инвариант того, что все пробелы
         * аккумулируются в верхушке истории. А также memtx_tx_story_link_top сделает
         * реплейс физически в индексе, чтобы сохранить еще один инвариант - верхушка
         * списка всегда представлена фиизически в индексе. Для этого в качестве
		 * is_new_tuple передается false.
         */
		memtx_tx_story_link_top(old_story, story, idx, false);
	}
}

/**
 * Полностью отсоединяет @a story от всех цепочек, также отсоединяет
 * все стейтменты транзакции и удаляет связанные трекеры.
 */
static void
memtx_tx_story_full_unlink_on_space_delete(struct memtx_story *story)
{
	/* Извлекаем story из всех цепочек. */
	for (uint32_t i = 0; i < story->index_count; i++) {
		struct memtx_story_link *link = &story->link[i];
        /*
         * Если это верхушка. Странно, что не перенесли пробелы в новую верхушку
         * и не сделали замену в индексе. Скорее всего эта функция отвечает чисто
         * за отсоединение, а остальное делается в другом месте.
         */
		if (link->newer_story == NULL) {
			assert(link->in_index == NULL);
			memtx_tx_story_unlink(story, link->older_story, i);
		} else {
			/* Обычное извлечение вершины из двусвязного списка. */
			link->newer_story->link[i].older_story = link->older_story;
			if (link->older_story != NULL)
				link->older_story->link[i].newer_story = link->newer_story;
			link->older_story = NULL;
			link->newer_story = NULL;
		}
	}

	/* Отсоединяем от стейтментов. */
	if (story->add_stmt != NULL)
		memtx_tx_story_unlink_added_by(story, story->add_stmt);
    /* Отсоединяем от себя все стейтменты, которые удалили наш тапл (нашу story). */
	while (story->del_stmt != NULL)
		memtx_tx_story_unlink_deleted_by(story, story->del_stmt);
	/* on_space_delete => gap'ы можно удалять, а не перемещать в новую верхушку. */
	for (uint32_t i = 0; i < story->index_count; i++) {
		struct rlist *read_gaps = &story->link[i].read_gaps;
		while (!rlist_empty(&story->link[i].read_gaps)) {
			struct inplace_gap_item *item = rlist_first_entry(read_gaps, struct inplace_gap_item, in_read_gaps);
			//memtx_tx_delete_gap(item);
			memtx_tx_inplace_gap_item_delete(item);
		}
	}
	/*
	 * Удаляем все трекеры, потому что они указывают на story,
     * которую мы собираемся удалить. Интересно, почему трекеры
     * не удалили, а просто убрали из списков.
     * Кто в последствии освободит память из под них?
	 */
	while (!rlist_empty(&story->reader_list)) {
		struct tx_read_tracker *tracker =
			rlist_first_entry(&story->reader_list, struct tx_read_tracker, in_reader_list);
		rlist_del(&tracker->in_reader_list);
		rlist_del(&tracker->in_read_set);
	}
}

static struct memtx_story *
memtx_tx_story_find_top(struct memtx_story *story, uint32_t ind)
{
	while (story->link[ind].newer_story != NULL)
		story = story->link[ind].newer_story;
	return story;
}

static bool
memtx_tx_tuple_key_is_excluded(struct tuple *tuple, struct index *index, key_def *def)
{
	return false;
}

/**
 * Отсоединить @a story от всех цепочек и удалить соостветствующий тапл из
 * индекса, если требуется: используется при итерации gc и сохраняет инвариант
 * верхушки цепочки (в отличие от `memtx_tx_story_full_unlink_on_space_delete`).
 * 
 * Замечание: не отсоединяет стейтменты и тректеры - ожидается, что они будут
 * отсутствовать в момент вызова, иначе gc не отсоединит story.
 */
static void
memtx_tx_story_full_unlink_story_gc_step(struct memtx_story *story)
{
	for (uint32_t i = 0; i < story->index_count; i++) {
		struct memtx_story_link *link = &story->link[i];
		if (link->newer_story == NULL) {
			/*
			 * Верхушка цепочки, а значит выполняется одно из двух:
             * либо story->tuple находится в индексе либо story откатили.
             * Если story фактически удаляет тапл и этот story представлен в индексе,
			 * он должен быть удален из индекса.
			 */
			assert(link->in_index != NULL);
			/*
             * Здесь мы не заменяем тапл на предыдущий, а просто удаляем тот,
             * который находится там сейчас. Мы предполагаем, что link->older_story
             * является NULL. Если бы это было не так, то мы нарушили бы инвариант
             * и новая верхушка не была бы представлена в индексе после удаления.
             * О том, что link->older_story == NULL должен позаботиться вызывающий. То есть
             * gc удаляет верхушку в последнюю очередь, когда уже удалил все, что
             * находится перед ней.
			 */
			assert(link->older_story == NULL);
            // странный if, мы уже ассертили это выше ---+
            //                                           |
            //                          vvvvvvvvvvvvvvvvvvvvvv
			//if (story->del_psn > 0 && link->in_index != NULL) {

            /*
             * Существует prepared транзакция, которая удалила данный тапл.
			 * 
			 * Удаления в отличие от вставок не происходят сразу.
			 * Удалением занимается GC, фактический реплейс на NULL в индексе делает он,
			 * и, как видно, делает это только, когда дошел до верхушки.
			 * 
			 * В первой версии был хороший комментарий в этом месте:
			 * We are at the top of the chain. That means that story->tuple is in index.
			 * If the story is actually delete the tuple, it must be deleted from index.
             */
            if (story->del_psn > 0) {
                struct index *index = link->in_index;
				struct tuple *removed/*, *unused*/;
				if (index_replace(index, story->tuple, NULL, DUP_INSERT, &removed/*, &unused*/) != 0) {
					/*panic*/fprintf(stderr, "failed to rollback change");
					exit(1);
				}
				//key_def *key_def = index->def->_key_def;
				assert(story->tuple == removed || (removed == NULL/* && memtx_tx_tuple_key_is_excluded(story->tuple, index, key_def)*/));
				//(void)key_def;
				link->in_index = NULL;
				/* Сейчас нам это не важно. */
				//if (i == 0)
				//	memtx_tx_unref_from_primary(story);
			}
            /* Отсоединили. */
			memtx_tx_story_unlink(story, link->older_story, i);
		} else {
			/* Обычное извлечение вершины из двусвязного списка. (копипаста кода выше) */
			link->newer_story->link[i].older_story = link->older_story;
			if (link->older_story != NULL)
				link->older_story->link[i].newer_story = link->newer_story;
			link->older_story = NULL;
			link->newer_story = NULL;
		}
	}
}

void
memtx_tx_story_gc_step()
{
	if (txm.traverse_all_stories == &txm.all_stories) {
		/* Дошли до конца, делаем шаг вперед и выходим. */
		txm.traverse_all_stories = txm.traverse_all_stories->next;
		return;
	}

	/*
	 * Значение по умолчанию — txn_next_psn (больше, чем у всех prepared транзакций
	 * в данный момент). Если в read view нет транзакций, то никакие транзакции не
	 * будут ошибочно отмечены, как MEMTX_TX_STORY_READ_VIEW.
	 */
	int64_t lowest_rv_psn = txn_next_psn;
	if (!rlist_empty(&txm.read_view_txns)) {
		struct txn *txn = rlist_first_entry(&txm.read_view_txns, struct txn, in_read_view_txns);
		assert(txn->rv_psn != 0);
		lowest_rv_psn = txn->rv_psn;
	}

	struct memtx_story *story = rlist_entry(txm.traverse_all_stories, struct memtx_story, in_all_stories);
	txm.traverse_all_stories = txm.traverse_all_stories->next;

	/*
	 * Порядок, в котором проверяются условия очень важен.
	 * См. описание enum memtx_tx_story_status.
	 */
	if (story->add_stmt != NULL || story->del_stmt != NULL || !rlist_empty(&story->reader_list)) {
		memtx_tx_story_set_status(story, MEMTX_TX_STORY_USED);
		/* Story напрямую используется какими-то транзакциями. */
		return;
	}
	if (story->add_psn >= lowest_rv_psn || story->del_psn >= lowest_rv_psn) {
		memtx_tx_story_set_status(story, MEMTX_TX_STORY_READ_VIEW);
		/* 
		 * Story может быть использована в read view. Не понятно, почему мы
		 * проверяем ..._psn >= lowest_rv_psn, а не _psn <= greatest_rv_psn.
		 */
		return;
	}
	for (uint32_t i = 0; i < story->index_count; i++) {
		struct memtx_story_link *link = &story->link[i];
		if (link->newer_story == NULL) { //если это верхушка
			assert(link->in_index != NULL);
			/*
			 * Тикет: https://github.com/tarantool/tarantool/issues/7490
			 *
			 * Фикс: https://github.com/tarantool/tarantool/commit/c8eccfbbc98264d73e6f3206bbfbee74543d54db
			 *
			 * Мы могли бы отсоединить этот тапл (и даже удалить его
			 * из индекса если story->del_psn > 0), но мы не можем
			 * сделать это, т.к. после этого `link->older_story`
			 * станет верхушкой цепочки, но не будет представлено
			 * в индексе, что нарушает инвариант.
			 */
			if (link->older_story != NULL) {
				memtx_tx_story_set_status(story, MEMTX_TX_STORY_USED);
				return;
			}
		}
		/*
		 * Тикет: https://github.com/tarantool/tarantool/issues/7712
		 *
		 * Фикс: https://github.com/tarantool/tarantool/commit/7b0baa57909cc9129705785e317a687f70927e27
		 * memtx: fix loss of committed tuple in secondary index
		 * 
		 * Конкурентные транзакции могут пытаться вставлять таплы, которые пересекаются только
		 * по частям вторичного индекса: в этом случае, когда один из них preparing, остальные
		 * конфликтуют, но закоммиченная story не сохранится (т.к. конфликтующие стейтменты не
		 * добавляются в список del_stmt закоммиченной story, в отличие от первичного индекса)
		 * и теряется после сборки мусора: сохраняйте истории, если в цепочке истории вторичных
		 * индексов есть более новая незафиксированная история.
		 */
		else if (i > 0 && link->newer_story->add_stmt != NULL) {
			/*
			 * Нам необходимо сохранить историю, так как более новая история может
			 * быть откачена (это поддерживается в списке del_stmt в случае
			 * первичного индекса).
			 */
			memtx_tx_story_set_status(story, MEMTX_TX_STORY_USED);
			return;
		}
		if (!rlist_empty(&link->read_gaps)) {
			memtx_tx_story_set_status(story, MEMTX_TX_STORY_TRACK_GAP);
			/* Story используется для отслеживания пробелов. */
			return;
		}
	}
	/*
	 * Отсоединяем и удаляем story. Возможно удаляем соответствующий тапл
	 * из индекса, если это верхушка и story->del_psn > 0.
	 */
	memtx_tx_story_full_unlink_story_gc_step(story);
	memtx_tx_story_delete(story);
}

void
memtx_tx_story_gc()
{
	for (size_t i = 0; i < txm.must_do_gc_steps; i++)
		memtx_tx_story_gc_step();
	txm.must_do_gc_steps = 0;
}

/**
 * Проверяем что вставка тапла (появление соотв. @a story видимо для транзакции @a txn.
 * @param is_prepared_ok - устраивает нас prepared, не confirmed или нет.
 * @param own_change - return true если @txn сама этот вставила. В этом случае возвращается true
 */
static bool
memtx_tx_story_insert_is_visible(struct memtx_story *story, struct txn *txn, bool is_prepared_ok, bool *is_own_change)
{
	*is_own_change = false;

    /*
     * Стейтмент контролируется менеджером, он не закоммичен.
     * Возможно он уже является prepared. Но если он наш, то мы
     * в любом случае его видим, независимо от is_prepared_ok.
     */
	if (story->add_stmt != NULL && story->add_stmt->txn == txn) {
		*is_own_change = true;
		return true;
	}

	int64_t rv_psn = INT64_MAX;
	if (txn != NULL && txn->rv_psn != 0)
		rv_psn = txn->rv_psn;

    /* Транзакция prepared и находится в read view в зоне нашей видимости. */
	if (is_prepared_ok && story->add_psn != 0 && story->add_psn < rv_psn)
		return true;

    /*
     * Транзакция закоммичена и находится в read view в зоне нашей видимости. 
     * Пока не совсем понятно, как закоммиченная транзакция может не находиться
     * в зоне нашей видимости, разберемся позже.
     */
	if (story->add_psn != 0 && story->add_stmt == NULL &&
	    story->add_psn < rv_psn)
		return true;
    
    /* Транзакция добавлена давно неизвестно кем. Её видят все. */
	if (story->add_psn == 0 && story->add_stmt == NULL)
		return true; /* Added long time ago. */

	return false;
}

/**
 * Проверяем, что удаление тапла (прекращение действия соотв. @a story видимо для транзакции @a txn.
 * @param is_prepared_ok - устраивает нас prepared, не confirmed или нет.
 * @param own_change - return true если @txn сама этот вставила. В этом случае возвращается true
 */
static bool
memtx_tx_story_delete_is_visible(struct memtx_story *story, struct txn *txn, bool is_prepared_ok, bool *is_own_change)
{
	*is_own_change = false;

    /*
     * Аналогично, проверяем, что данная транзакция является одной из тех,
     * кто удалил данный тапл.
     */
	struct txn_stmt *dels = story->del_stmt;
	while (dels != NULL) {
		if (dels->txn == txn) {
			*is_own_change = true;
			return true;
		}
		dels = dels->next_in_del_list;
	}

	int64_t rv_psn = INT64_MAX;
	if (txn != NULL && txn->rv_psn != 0)
		rv_psn = txn->rv_psn;

    /* Аналогично, транзакция prepared и находится в read view в зоне нашей видимости. */
	if (is_prepared_ok && story->del_psn != 0 && story->del_psn < rv_psn)
		return true;

    /*
     * Аналогично.
     * Транзакция закоммичена и находится в read view в зоне нашей видимости. 
     * Пока не совсем понятно, как закоммиченная транзакция может не находиться
     * в зоне нашей видимости, разберемся позже.
     */
	if (story->del_psn != 0 && story->del_stmt == NULL &&
	    story->del_psn < rv_psn)
		return true;

    /* Не может быть такого, что story удалена давно и неизвестно, кто это сделал. */
	return false;
}

/**
 * Сканировать историю начиная с @a story в индексе @a index в поисках
 * видимого тапла @a visible_tuple.
 * @param is_prepared_ok - устраивает нас prepared, не confirmed или нет.
 * @param own_change - return true если @txn сама этот вставила. В этом случае возвращается true
 */
static void
memtx_tx_story_find_visible_tuple(
    struct memtx_story *story, struct txn *txn, uint32_t index, bool is_prepared_ok, struct tuple **visible_tuple, bool *is_own_change)
{
	for (; story != NULL; story = story->link[index].older_story) {
		assert(index < story->index_count);
        /*
         * Пока не понятно, как может быть видимо удаление, но не видна вставка следующего. Возможно,
         * это предусмотрено именно для того случая, когда тапл соотв. story A был просто удален, тогда
         * в конце не будет никакой отдельной story B про удаление, просто у A будет выставлен del_stmt.
         */
		if (memtx_tx_story_delete_is_visible(story, txn, is_prepared_ok, is_own_change)) {
			*visible_tuple = NULL;
			return;
		}
		if (memtx_tx_story_insert_is_visible(story, txn, is_prepared_ok, is_own_change)) {
			*visible_tuple = story->tuple;
			return;
		}
	}
	*visible_tuple = NULL;
}

/**
 * Отмечаем факт, что транзакция прочитала конкретный тапл. Мы должны
 * будем гарантировать, что транзакция сериализуется так, что в момент,
 * когда она начинает выполняться, она действительно прочитает именно этот
 * тапл.
 */
static void
memtx_tx_track_read_story(struct txn *txn, struct memtx_space *space, struct memtx_story *story);

/**
 * То же самое, что функция выше, но здесь мы хотим зафиксировать тот факт,
 * что транзакция ничего не прочитала в определенном месте.
 */
static void
memtx_tx_track_story_gap(struct txn *txn, struct memtx_story *story, uint32_t ind);

static struct point_hole_item *
point_hole_item_new()
{
	return (struct point_hole_item *)malloc(sizeof(struct point_hole_item));
}

/**
 * Deletes the point hole item. The deletion of the item from the point hole
 * storage is handled separately.
 */
static void
point_hole_item_delete(struct point_hole_item *object)
{
	rlist_del(&object->ring);
	rlist_del(&object->in_point_holes_list);
	free(object);
}

/**
 * Переносим gap'ы из мапчика в @a story только что вставленного тапла
 * @a new tuple в индекс @a ind. Это нужно если и только если
 * это была реальная фактическая вставка - никакой тапл не был заменен.
 * Потому что в этом и только в этом случае в мапчике может лежать какой-то
 * непустой список point_hole_item'ов.
 */
static void
memtx_tx_handle_point_hole_write(struct memtx_space *space, struct memtx_story *story, uint32_t ind)
{
	/*
	 * Нам важно, что данная story - это верхушка, потому что мы будем добавлять
	 * элементы в её read_gaps (memtx_tx_track_story_gap должна вызываться только
	 * на верхушке).
	 */
	assert(story->link[ind].newer_story == NULL);
	struct index *index = &space->index[ind];
	struct mh_point_holes_t *ht = txm.point_holes;
	struct point_hole_key key;
	key.index = index;
	key.tuple = story->tuple;
	//key.func_key = NULL;
	//if (index->def->_key_def->for_func_index)
	//	key.func_key = memtx_tx_tuple_func_key(story->tuple, index);
	mh_int_t pos = mh_point_holes_find(ht, &key, 0);
	if (pos == mh_end(ht))
		return;
	struct point_hole_item *item = *mh_point_holes_node(ht, pos);
	/*
	 * Remove from the storage before deleting the element because
	 * it still can be used under the hood.
	 */
	mh_point_holes_del(ht, pos, 0);

	bool has_more_items;
	do {
		memtx_tx_track_story_gap(item->txn, story, ind);
		struct point_hole_item *next_item = rlist_entry(item->ring.next, struct point_hole_item, ring);
		has_more_items = next_item != item;
		point_hole_item_delete(item);
		item = next_item;
	} while (has_more_items);
}

static bool
memtx_tx_tuple_matches(key_def *def, struct tuple *tuple, int key)
{
	return (tuple_compare_with_key(tuple, key, def) == 0);
}

/**
 * Сохранить в TX менеджере информацию о том, что транзакция @txn
 * прочитала тапл @tuple в спейсе @space.
 *
 * NB: Может вызвать сборку мусора.
 */
static void
memtx_tx_track_read(struct txn *txn, struct memtx_space *space, struct tuple *tuple);

/**
 * Проверить, что произведенные в индексах замены не нарушают никаких ограничений
 * и правил замены.
 *
 * `is_own_change` выставляется в true если `old_tuple` был изменен (
 * удален или добавлен) транзакцией данного стейтмента.
 */
static int
check_dup(struct txn_stmt *stmt, struct tuple *new_tuple, struct tuple **directly_replaced, struct tuple **old_tuple, enum dup_replace_mode mode, bool *is_own_change)
{
	struct memtx_space *space = stmt->space;
	struct txn *txn = stmt->txn;

	struct tuple *visible_replaced;
	if (directly_replaced[0] == NULL ||
        /*
         * TUPLE_IS_DIRTY означает, что у тапла есть какая-то история и нужно
         * внимательно смотреть, какая версия видна нам.
         */
	    !tuple_has_flag(directly_replaced[0], TUPLE_IS_DIRTY)) {
		*is_own_change = false;
		visible_replaced = directly_replaced[0];
	} else {
		struct memtx_story *story = memtx_tx_story_get(directly_replaced[0]);
		memtx_tx_story_find_visible_tuple(story, txn, 0, true, &visible_replaced, is_own_change);
	}

    /* old_tuple' == old_tuple, dup_tuple == visible_replaced */
	if (index_check_dup(&space->index[0], *old_tuple, new_tuple, visible_replaced, mode) != 0) {
        /*
         * Если получили ошибку по первичному ключу, трекаем чтение. Нужно подумать отдельно,
         * почему именно так.
         */
		memtx_tx_track_read(txn, space, visible_replaced);
		return -1;
	}

	for (uint32_t i = 1; i < space->index_count; i++) {
		/* Проверяем, что visible tuple == NULL или такой же как в первичном индексе. */
		if (directly_replaced[i] == NULL)
			continue; /* NULL is OK in any case. */

		struct tuple *visible;
		if (!tuple_has_flag(directly_replaced[i], TUPLE_IS_DIRTY)) {
			visible = directly_replaced[i];
		} else {
			/* У тапла есть какая-то нетривиальная история. */
			struct memtx_story *story = memtx_tx_story_get(directly_replaced[i]);
			bool unused;
			memtx_tx_story_find_visible_tuple(story, txn, i, true, &visible, &unused);
		}
        /* old_tuple' == visible_replaced, dup_tuple == visible */
		if (index_check_dup(&space->index[i], visible_replaced, new_tuple, visible, DUP_INSERT) != 0) {
            /* Также трекаем чтение при первой же ошибке и выходим. */
			memtx_tx_track_read(txn, space, visible);
			return -1;
		}
	}
    /*
	 * Выставили old_tuple. Мне, кажется, очень неудачным решением то,
	 * что check_dup здесь еще занимается выставлением visible_replaced
	 * куда-то.
	 */
	*old_tuple = visible_replaced;
	return 0;
}

/* Добавляет inplace_gap_item в story::link[ind] */
static void
memtx_tx_track_story_gap(struct txn *txn, struct memtx_story *story, uint32_t ind)
{
	assert(story->link[ind].newer_story == NULL);
	assert(txn != NULL);
	struct inplace_gap_item *item = memtx_tx_inplace_gap_item_new(txn);
	rlist_add(&story->link[ind].read_gaps, &item->in_read_gaps);
}

static void
memtx_tx_history_add_stmt_prepare_result(struct tuple *old_tuple, struct tuple **result)
{
	*result = old_tuple;
	//if (*result != NULL) {
	//	tuple_ref(*result);
	//}
}

/**
 * Вызывается только из @sa memtx_tx_history_add_stmt в случае, когда new_tuple != NULL.
 * new_tuple != NULL возможно в случаях:
 * REPLACE, тогда old_tuple == NULL, т.к. пока не известно, что мы зареплейсим.
 * INSERT, тогда old_tuple == NULL, т.к. по данному ключу тапла быть не должно.
 * UPDATE, тогда old_tuple != NULL, этот тапл мы собираемся обновить.
 */
static int
memtx_tx_history_add_insert_stmt(struct txn_stmt *stmt, struct tuple *old_tuple, struct tuple *new_tuple, enum dup_replace_mode mode, struct tuple **result)
{
	assert(new_tuple != NULL);
	struct memtx_space *space = stmt->space;

	/* Создаем story, тапл становится DIRTY. */
	struct memtx_story *add_story = memtx_tx_story_new(space, new_tuple);

	/*
     * Реплейсим физически в каждом индексе, запоминаем при этом какой элемент
     * заменили (directly_replaced[i]), и какой элемент был следующим (direct_successor[i]).
     */
	struct tuple *directly_replaced[space->index_count];
	//struct tuple *direct_successor[space->index_count];
	uint32_t directly_replaced_count = 0;
	for (uint32_t i = 0; i < space->index_count; i++) {
		struct index *index = &space->index[i];
		struct tuple **replaced = &directly_replaced[i];
		//struct tuple **successor = &direct_successor[i];
		*replaced /*= *successor*/ = NULL;
		if (index_replace(index, NULL, new_tuple, DUP_REPLACE_OR_INSERT, replaced/*, successor*/) != 0)
		{
			directly_replaced_count = i;
			goto fail;
		}
	}
	directly_replaced_count = space->index_count;

	/* Проверяем, что все условия удовлетворены, а также получаем видимый old_tuple. */
	bool is_own_change = false;
	int rc = check_dup(stmt, new_tuple, directly_replaced, &old_tuple, mode, &is_own_change);
	if (rc != 0)
		goto fail;
    /* Выставили is_own_change для стейтмента. */
	stmt->is_own_change = is_own_change;

	/* Запомнили, что эта story была создана этим стейтментом. */
	memtx_tx_story_link_added_by(add_story, stmt);

	/* Создаем, если необходимо, story для замененного тапла. */
	struct tuple *next_pk = directly_replaced[0];
	struct memtx_story *next_pk_story = NULL;
	if (next_pk != NULL && tuple_has_flag(next_pk, TUPLE_IS_DIRTY)) {
		next_pk_story = memtx_tx_story_get(next_pk);
	} else if (next_pk != NULL) {
		next_pk_story = memtx_tx_story_new(space, next_pk);
	}

	/* Collect conflicts or form chains. */
	for (uint32_t i = 0; i < space->index_count; i++) {
		struct tuple *next = directly_replaced[i];
		//struct tuple *succ = direct_successor[i];
		struct index *index = &space->index[i];
		bool tuple_is_excluded = memtx_tx_tuple_key_is_excluded(new_tuple, index, &index->_key_def);
		if (next == NULL && !tuple_is_excluded) {
			/* Collect conflicts. */
			/*
			 * memtx_tx_handle_gap_write не обрабатывает inplace gap'ы,
			 * а нас пока интересуют именно они.
			 */
			//memtx_tx_handle_gap_write(space, add_story, succ, i);

			/*
			 * Данная story - первая в цепочке, а значит, нужно перенести в неё
			 * те пробелы, которые хранятся в нас в мапчике. (Удалить из мапчика
			 * и перенести в story->link[i].read_gaps).
			 */
			memtx_tx_handle_point_hole_write(space, add_story, i);
			memtx_tx_story_link_top(add_story, NULL, i, true);
		}
		if (next != NULL) {
			/* Form chains. */
			struct memtx_story *next_story = next_pk_story;
			if (next != next_pk) {
				assert(tuple_has_flag(next, TUPLE_IS_DIRTY));
				next_story = memtx_tx_story_get(next);
			}
			memtx_tx_story_link_top(add_story, next_story, i, true);
		}
	}

	/*
	 * Сейчас old_tuple указывает на тапл, который фактически был
	 * заменен данным стейтментом (check_dup вызвал find_visible, а затем
	 * сделал *old_tuple = visible). Найдем его story и соединим с данным
	 * стейтметом. Обозначим, что данный стейтмент удалил данный тапл.
	 */
	struct memtx_story *del_story = NULL;
	if (old_tuple != NULL) {
		assert(tuple_has_flag(old_tuple, TUPLE_IS_DIRTY));
		if (old_tuple == next_pk)
			del_story = next_pk_story;
		else
			del_story = memtx_tx_story_get(old_tuple);
		memtx_tx_story_link_deleted_by(del_story, stmt);
	}

	/*
	 * В DUP_INSERT мы не должны фактически заменить никакой тапл (нужно гарантировать,
	 * что перед выполнением транзакции в сериализованом порядке, на этом месте будет
	 * читаться NULL). Сейчас это выполняется (проверено в check_dup), но мы должны
	 * предотвратить дальнейшую вставку в это место, то есть мы должны track gap.
	 * 
	 * В случае реплейса нам обычно не важно наличие или отсутствие тапла в момент вставки,
	 * а также, какой именно тапл был заменен в результате. Но в случае если там был триггер
	 * before replace или on_replace, то он видел тапл, который был заменен, поэтому в данном
	 * случае придется трекать чтение story, чтобы гарантировать, что тапл останется тем же.
	 */
	if (!is_own_change &&
	    (mode == DUP_INSERT/* ||
	     space_has_before_replace_triggers(stmt->space) ||
	     space_has_on_replace_triggers(stmt->space)*/)) {
		assert(mode != DUP_INSERT || del_story == NULL);
		if (del_story == NULL) {
			memtx_tx_track_story_gap(stmt->txn, add_story, 0);
		} else {
			memtx_tx_track_read_story(stmt->txn, space, del_story);
		}
	}

	memtx_tx_history_add_stmt_prepare_result(old_tuple, result); //*result = old_tuple;
	return 0;

fail:
	/* Откатываем то, что уже успели применить. */
	for (uint32_t i = directly_replaced_count - 1; i + 1 > 0; i--) {
		struct index *index = &space->index[i];
		struct tuple *unused;
		if (index_replace(index, new_tuple, directly_replaced[i], DUP_INSERT, &unused/*, &unused*/) != 0) {
			//diag_log();
			/*panic*/fprintf(stderr, "failed to rollback change");
			exit(1);
		}
	}
	memtx_tx_story_delete(add_story);
	return -1;
}

/**
 * Вызывается только из @sa memtx_tx_history_add_stmt в случае, когда new_tuple == NULL.
 * (DELETE)
 */
static int
memtx_tx_history_add_delete_stmt(struct txn_stmt *stmt, struct tuple *old_tuple, struct tuple **result)
{
	/*
	 * Найти удаленную story и соединить со стейтментом.
	 *
	 * Специфический API функции space->replace требует в качестве аргумента
	 * old_tuple, который можно получить только через mvcc clirifying. Это
	 * означает, что история old_tuple должна быть уже создана и уже содержит
	 * запись о чтении этой транзакцией. Вот почему мы ожидаем, что old_tuple
	 * будет DIRTY и не устанавливаем трекер чтения, как было бы логически
	 * правильно в этой функции, что-то вроде этого:
	 * memtx_tx_track_read_story(stmt->txn, stmt->space, del_story)
	 */
	assert(tuple_has_flag(old_tuple, TUPLE_IS_DIRTY));
	struct memtx_story *del_story = memtx_tx_story_get(old_tuple);
	if (del_story->add_stmt != NULL) /* in-progress либо prerared, но не закоммичена. */
		/* выставили is_own_change. */
		stmt->is_own_change = del_story->add_stmt->txn == stmt->txn;
	/* запомнили, что именно этот стейтмент удалил данную story. */
	memtx_tx_story_link_deleted_by(del_story, stmt);

	/* Обработка каунтов нам пока не интересна. */
	//struct memtx_space *space = stmt->space;
	//for (uint32_t i = 0; i < space->index_count; i++) {
	//	struct index *index = space->index[i];
	//	if (!memtx_tx_tuple_key_is_excluded(del_story->tuple, index,
	//					    index->def->_key_def)) {
	//		memtx_tx_handle_counted_write(space, del_story, i);
	//	}
	//}

	/* Статистика пока тоже не влияет на суть. */
	//if (!del_story->tuple_is_retained)
	//	memtx_tx_story_track_retained_tuple(del_story);

	memtx_tx_history_add_stmt_prepare_result(old_tuple, result); //*result = old_tuple;
	return 0;
}

/* Обработка стейтмента. Вызывается только из memtx_space_replace_all_keys. */
int
memtx_tx_history_add_stmt(struct txn_stmt *stmt, struct tuple *old_tuple, struct tuple *new_tuple, enum dup_replace_mode mode, struct tuple **result)
{
	assert(stmt != NULL);
	/* ephemeral поддерживать не будем. */
	assert(stmt->space != NULL/* && !stmt->space->def->opts.is_ephemeral*/);
	assert(new_tuple != NULL || old_tuple != NULL);
	assert(new_tuple == NULL || !tuple_has_flag(new_tuple, TUPLE_IS_DIRTY));

	memtx_tx_story_gc();
	if (new_tuple != NULL)
		// см. выше
		return memtx_tx_history_add_insert_stmt(stmt, old_tuple, new_tuple, mode, result);
	else
		// см. выше
		return memtx_tx_history_add_delete_stmt(stmt, old_tuple, result);
}

/*
 * Абортим с конфликтом всех, кто прочитал эту story. См. memtx_tx_track_read_story,
 * чтобы понять, когда и почему пушим в этот список.
 */
static void
memtx_tx_abort_story_readers(struct memtx_story *story)
{
	struct tx_read_tracker *tracker, *tmp;
	rlist_foreach_entry_safe(tracker, &story->reader_list, in_reader_list, tmp)
		txn_abort_with_conflict(tracker->reader);
}

/*
 * Откатываем добавление story данным стейтментом.
 */
static void
memtx_tx_history_rollback_added_story(struct txn_stmt *stmt)
{
	struct memtx_story *add_story = stmt->add_story;
	struct memtx_story *del_story = stmt->del_story;

	/*
	 * В случае отката подготовленного оператора нам необходимо откатить
	 * preparation actions и прервать другие транзакции, которым удалось
	 * прочитать это подготовленное состояние..
	 */
	if (stmt->txn->psn != 0) { // prepared.
		/*
		 * В момент подготовки этого стейтмента было два возможных случая:
		 * * del_story != NULL: все in-progress транзакции, которые планировали
		 *   удалить del_story, были переприсоединены так, чтобы удалить add_story.
		 * * del_story == NULL: все in-progress транзакции, которые планировали
		 *   ничего не удалять, были переприсоединены так, чтобы удалить add_story.
		 * См. memtx_tx_history_prepare_insert_stmt для большего понимания.
		 * Учтите, что по задумке роллбека, все стейтменты откатываются в
		 * обратном порядке, и соответственно в этой точке может не быть
		 * стейтментов данной транзакции, которые удаляли эту add_story.
		 * Мы должны просканировать удаляющие стейтменты и переприсоединить их
		 * таким образом, чтобы они удаляли del_story, если она не NULL или не
		 * удаляли ничего в противном случае.
		 */
		struct txn_stmt **from = &add_story->del_stmt;
		while (*from != NULL) {
			struct txn_stmt *test_stmt = *from;
			assert(test_stmt->del_story == add_story);
			assert(test_stmt->txn != stmt->txn);
			assert(!test_stmt->is_own_change);
			assert(test_stmt->txn->psn == 0);

			/* Отсоединяем от add_story списка. */
			*from = test_stmt->next_in_del_list;
			test_stmt->next_in_del_list = NULL;
			test_stmt->del_story = NULL;

			if (del_story != NULL) {
				/* Присоединяем к del_story's списку. */
				memtx_tx_story_link_deleted_by(del_story, test_stmt);
			}
		}

		/* Откатываем присвоение psn. */
		add_story->add_psn = 0;
		if (del_story != NULL)
			del_story->del_psn = 0;

		/* Транзакции, читавшие данный story должны заабортиться. */
		memtx_tx_abort_story_readers(add_story);
	}

	/* Отсоединяем story от стейтмента. */
	memtx_tx_story_unlink_added_by(add_story, stmt);
	if (del_story != NULL)
		/* Если что-то удалили - тоже. */
		memtx_tx_story_unlink_deleted_by(del_story, stmt);

	/*
	 * Перенести историю в конец цепочки и отметить ее как удаленную давно
	 * (с очень низким del_psn). После этого история станет невидимой для
	 * любого читателя (это именно то, чего бы мы хотели) и все еще сможет
	 * хранить read set, если это необходимо.
	 */
	for (uint32_t i = 0; i < add_story->index_count; ) {
		struct memtx_story *old_story = add_story->link[i].older_story;
		if (old_story == NULL) {
			/* Эта story теперь самая первая, переходим на следующий индекс. */
			i++;
			continue;
		}
		memtx_tx_story_reorder(add_story, old_story, i);
	}
	add_story->del_psn = MEMTX_TX_ROLLBACKED_PSN; // psn = 1;
}

/* Абортим все транзакции, которые прочитали отсутствие @a story. */
static void
memtx_tx_abort_gap_readers(struct memtx_story *story)
{
	for (uint32_t i = 0; i < story->index_count; i++) {
		/*
		 * Здесь мы опираемся на инвариант, что все gap трекеры храняться в самой
		 * верхушке цепочки. Выше уже обсуждали наличие данного инварианта.
		 */
		struct memtx_story *top = memtx_tx_story_find_top(story, i);
		struct inplace_gap_item *item, *tmp;
		rlist_foreach_entry_safe(item, &top->link[i].read_gaps, in_read_gaps, tmp) {
			/* Пока что для понимания у нас все gap'ы - inplace. */
			//if (item->type != GAP_INPLACE)
			//	continue;
			txn_abort_with_conflict(item->txn);
		}
	}
}

/* Откатить удаление story данным стейтментом. */
static void
memtx_tx_history_rollback_deleted_story(struct txn_stmt *stmt)
{
	struct memtx_story *del_story = stmt->del_story;

	/*
	 * В случае отката подготовленного оператора нам необходимо откатить
	 * preparation actions и прервать другие транзакции, которым удалось
	 * прочитать это подготовленное состояние..
	 */
	if (stmt->txn->psn != 0) { // prepared.
		/*
		 * В момент подготовки удаления мы могли отсоединить другие транзакции,
		 * которые хотели перезаписать данный story. Теперь мы должны восстановить
		 * эту связь. Replace-like стейтменты можно найти в story-цепочке первичного ключа.
		 * К сожалению DELETE стейтменты не могут быть найдены, поскольку после отсоединения
		 * она не представлены в цепочках. Но к счастью by design все их транзакции, конечно,
		 * сконфликтовали из-за read-write конфликта и поэтому не важны для нас.
		 */
		struct memtx_story *test_story;
		for (test_story = del_story->link[0].newer_story;
		     test_story != NULL;
		     test_story = test_story->link[0].newer_story) {
			struct txn_stmt *test_stmt = test_story->add_stmt;
			/* Добавил туда же, откуда удалил. */
			if (test_stmt->is_own_change)
				continue;
			assert(test_stmt->txn != stmt->txn);
			/* Не понятно, чем это гарантируется. */
			assert(test_stmt->del_story == NULL);
			/* Не понятно, чем это гарантируется. */
			assert(test_stmt->txn->psn == 0);
			memtx_tx_story_link_deleted_by(del_story, test_stmt);
		}

		/* Откатываем присвоение psn. */
		del_story->del_psn = 0;

		/* Транзакции, которые прочитали отсутствие этой story, должны быть зааборчены. */
		memtx_tx_abort_gap_readers(del_story);
	}

	/* Отсоединяем story от стейтмента. */
	memtx_tx_story_unlink_deleted_by(del_story, stmt);
}

/**
 * Вызывается, когда add_story и del_story == NULL. Это называется empty.
 * 
 * Хелпер для отката пустого стейтмента - с ним не соединены никакие story.
 * Этот пустой стейтмент мог возникнуть по нескольким причинам:
 * 1. MVCC не создал stories для этого стейтмента. Это происходит, если спейс
 *    ephemeral или если стейтмент не удалил ничего. В этом случае хелпер не
 *    делает ничего.
 * 2. MVCC создал stories для стейтмента, но они были удалены по DDL -
 *    существует 3 типа транзакий. Первая - concurrent с DDL. Мы не должны откатывать
 *    их потому что мы уже обработали их на DDL. Вторая - сам DDL
 *    (`is_schema_changed` флаг выставлен) поскольку stories всех операций DML,
 *    которые произошли до DDL, были удалены. Мы должны откатывать такие стейтменты,
 *    т.к. теперь спейс содержит все свои таплы. Третий тип - транзакции,
 *    подготовленные до DDL. Мы также удалили их stories на DDL, поэтому здесь мы должны
 *    откатить их without stories if they have failed to commit.
 * 
 *    Пока не понятно вообще, что за покемон.
 */
void
memtx_tx_history_rollback_empty_stmt(struct txn_stmt *stmt)
{
	struct tuple *old_tuple = stmt->rollback_info.old_tuple;
	struct tuple *new_tuple = stmt->rollback_info.new_tuple;
	/* Не DDL и не prepared. */
	if (/*!stmt->txn->is_schema_changed && */stmt->txn->psn == 0)
		return;
	/* ephemeral поддерживать не будем. */
	if (/*stmt->space->def->opts.is_ephemeral ||*/
	    (old_tuple == NULL && new_tuple == NULL))
		return;
	for (size_t i = 0; i < stmt->space->index_count; i++) {
		struct tuple *unused;
		if (index_replace(&stmt->space->index[i], new_tuple, old_tuple, DUP_REPLACE_OR_INSERT, &unused/*, &unused*/) != 0) {
			/*panic*/fprintf(stderr, "failed to rebind story in index on "
			      "rollback of statement without story");
			exit(1);
		}
	}
	/* We have no stories here so reference bare tuples instead. */
	//if (new_tuple != NULL)
	//	tuple_unref(new_tuple);
	//if (old_tuple != NULL)
	//	tuple_ref(old_tuple);
}

void
memtx_tx_history_rollback_stmt(struct txn_stmt *stmt)
{
	/* Consistency asserts. */
	if (stmt->add_story != NULL) {
		assert(stmt->add_story->tuple == stmt->rollback_info.new_tuple);
		assert(stmt->add_story->add_psn == stmt->txn->psn);
	}
	if (stmt->del_story != NULL)
		assert(stmt->del_story->del_psn == stmt->txn->psn);
	/*
	 * Одновременно может существовать только один prepared стейтмент,
	 * удаляющие story. Видимо, когда препейрится удаляющий стейтмент,
	 * все остальные выкидываются из списка, но пока не понятно, что
	 * происходит с ними дальше.
	 */
	assert(stmt->txn->psn == 0 || stmt->next_in_del_list == NULL);

	/*
	 * Видимо memtx_tx_history_rollback_added_story обрабатывает и
	 * удаление сразу??? Ну да, кажется, это правда.
	*/
	if (stmt->add_story != NULL)
		memtx_tx_history_rollback_added_story(stmt);
	else if (stmt->del_story != NULL)
		memtx_tx_history_rollback_deleted_story(stmt);
	else
		memtx_tx_history_rollback_empty_stmt(stmt);
	assert(stmt->add_story == NULL && stmt->del_story == NULL);
}

/**
 * Откатываем или отправляем в read view всех читателей @a story,
 * за исключением транзакции @a writer, которая удалила данный story.
 */
static void
memtx_tx_handle_conflict_story_readers(struct memtx_story *story, struct txn *writer)
{
	struct tx_read_tracker *tracker, *tmp;
	rlist_foreach_entry_safe(tracker, &story->reader_list, in_reader_list, tmp) {
		if (tracker->reader == writer)
			continue;
		txn_send_to_read_view(tracker->reader, writer->psn);
	}
}

/**
 * Откатываем или отправляем в read view всех читателей @a story,
 * за исключением транзакции @a writer, которая удалила данный story.
 */
static void
memtx_tx_handle_conflict_gap_readers(struct memtx_story *top_story, uint32_t ind, struct txn *writer)
{
	assert(top_story->link[ind].newer_story == NULL);
	struct inplace_gap_item *item, *tmp;
	rlist_foreach_entry_safe(item, &top_story->link[ind].read_gaps, in_read_gaps, tmp) {
		if (item->txn == writer/* || item->type != GAP_INPLACE*/)
			continue;
		txn_send_to_read_view(item->txn, writer->psn);
	}
}

/**
 * Хелпер функции memtx_tx_history_prepare_stmt. Предусмотрен для случая
 * stmt->add_story != NULL, например, REPLACE, INSERT, UPDATE etc.
 */
static void
memtx_tx_history_prepare_insert_stmt(struct txn_stmt *stmt)
{
	struct memtx_story *story = stmt->add_story;
	assert(story != NULL);
	/**
	 * История ключа в индексе может состоять из нескольких stories.
	 * Список stories начинается с dirty тапла в жтом индексе.
	 * Список начинается с нескольких (или нуля) stories, которые были добавлены
	 * in-progress транзакциями, затем идут несколько (или 0) prepared stories,
	 * после которых далее идут несколько (или 0) закоммиченных stories,
	 * после которых идут несколько откаченных stories.
	 * Мы имеем следующее totally ordered множество stories:
	 *
	 * —————————————————————————————————————————————————> serialization time
	 * |- - - - - - - -|— — — — — -|— — — — — |— — — — — — -|— — — — — — — -
	 * | Rolled back   | Committed | Prepared | In-progress | One dirty
	 * |               |           |          |             | story in index
	 * |- - - - - - - -|— — — — — -| — — — — —|— — — — — — -|— — — — — — — —
	 *
	 * Если стейтмент становится prepared, story, которую он добавил, должна быть
	 * погружена на уровень prepared stories.
	 */
	for (uint32_t i = 0; i < story->index_count; ) {
		struct memtx_story *old_story = story->link[i].older_story;
		if (old_story == NULL || old_story->add_psn != 0 || old_story->add_stmt == NULL) {
			/* Предыдущая story - prepared, поэтому можно перейти к след. индексу. */
			i++;
			continue;
		}
		memtx_tx_story_reorder(story, old_story, i);
	}

	/* Consistency asserts. */
	{
		assert(story->del_stmt == NULL ||
		       story->del_stmt->next_in_del_list == NULL);
		struct memtx_story *old_story = story->link[0].older_story;
		if (stmt->del_story == NULL)
			assert(old_story == NULL || old_story->del_psn != 0);
		else
			assert(old_story == stmt->del_story);
		(void)old_story;
	}

	/*
	 * Выставить более новые (in-progress) стейтменты в первичной цепочке,
	 * как удаляющие данную story.
	 */
	if (stmt->del_story == NULL) {
		/*
		 * Этот стейтмент ничего не удалил. Это означает что до
		 * this preparation там в индексе не было видимого тапла,
		 * а теперь он там появится.
		 * Следовательно, могут быть какие-то in-progress транзакции,
		 * которые думают, что они ничего не реплейсят. Им нужно сообщить,
		 * что теперь они реплейсят данный тапл.
		 */
		struct memtx_story *test_story;
		for (test_story = story->link[0].newer_story; //идем вперед по первичной цепочке
		     test_story != NULL;
		     test_story = test_story->link[0].newer_story) {
			struct txn_stmt *test_stmt = test_story->add_stmt;
			if (test_stmt->is_own_change)
				continue;
			assert(test_stmt->txn != stmt->txn);
			/*
			 * стейтменты ничего не удаляли (не вызывали DELETE на данном ключе),
			 * иначе у них is_own_change был бы true.
			 */
			assert(test_stmt->del_story == NULL);
			/*
			 * почему они не были prepared - понятно, мы только что сдвинулись в конец prepared
			 * впереди нас только in-progress.
			 */
			assert(test_stmt->txn->psn == 0);
			memtx_tx_story_link_deleted_by(story, test_stmt);
		}
	} else {
		/*
		 * Этот стейтмент пореплейсил older story. Это означает, что до
		 * this preparation в этом месте индекса присутствовал другой
		 * видимый тапл.
		 * Следовательно, могут быть какие-то in-progress транзакции,
		 * которые думают, что они удалили этот другой тапл. Им нужно сообщить,
		 * что они больше не реплейсят данный тапл.
		 */
		struct txn_stmt **from = &stmt->del_story->del_stmt;
		while (*from != NULL) {
			struct txn_stmt *test_stmt = *from;
			assert(test_stmt->del_story == stmt->del_story);
			if (test_stmt == stmt) {
				/* Скипаем себя. */
				from = &test_stmt->next_in_del_list;
				continue;
			}
			assert(test_stmt->txn != stmt->txn);
			assert(test_stmt->txn->psn == 0);

			/* Убираем из списка old story. */
			*from = test_stmt->next_in_del_list;
			test_stmt->next_in_del_list = NULL;
			test_stmt->del_story = NULL;

			/* Переставляем в список story's. */
			memtx_tx_story_link_deleted_by(story, test_stmt);
		}
	}

	/* Обрабатываем основные конфликты. */
	if (stmt->del_story != NULL) {
		/*
		 * Story stmt->del_story прекращает действовать. Каждая транзакция,
		 * которая зависит от этого, должны уйти в read view или быть зааборчена.
		 * 
		 * Всем, кто удаляли этот тапл, просто переставили ссылки, а тех, кто прочитали
		 * отправляем в read view либо откатываем. В принципе, наверное, логично.
		 */
		memtx_tx_handle_conflict_story_readers(stmt->del_story, stmt->txn);
	} else {
		/*
		 * Тапл был вставлен на пустое место. Каждая транзакция, зависящая
		 * от отсутствия тапла в этом месте (в каком-либо индексе), должна отправиться
		 * в read view или быть зааборчена.
		 * Мы чекаем только первичный индекс здесь, остальные вторичные чекнем ниже.
		 */
		struct memtx_story *top_story = memtx_tx_story_find_top(story, 0);
		/*
		 * У memtx_tx_handle_conflict_gap_readers немного другой интерфейс, не такой
		 * как у memtx_tx_abort_gap_readers. memtx_tx_handle_conflict_gap_readers принимает
		 * верхушку, потому что иногда она известна снаружи. В memtx_tx_abort_gap_readers
		 * можно передавать любую story из цепочки, он сам вызовет memtx_tx_story_find_top
		 * и найдет верхушку.
		 */
		memtx_tx_handle_conflict_gap_readers(top_story, 0, stmt->txn);
	}

	/* Обработка конфликтов во вторичных индексах. */
	for (uint32_t i = 1; i < story->index_count; i++) {
		/*
		 * Обработка вторичных cross-write конфликтов. Этот случай
		 * слишком сложен и заслуживает пояснения на примере.
		 * Представим спейс с первичным ключом (pk) по первому полю
		 * и вторичным индексом (sk) по второму полю.
		 * Представим себе 3 in-progress транзакции, которые выполняют
		 * реплейсы {1, 1, 1}, {2, 1, 2} и {1, 1, 3} соответственно.
		 * Что должно произойти, когда первая транзакция коммитится?
		 * Обе другие транзакции пересекаются с текущей во вторичном ключе.
		 * Но вторая транзакция с {2, 1, 2} должна быть зааборчена
		 * (или отправлена в read view) из-за конфликта: она вводит
		 * дубликат по вторичному ключу.
		 * С другой стороны третья транзакция с таплом {1, 1, 3} имеет
		 * право на существование, т.к. перезаписывает {1, 1, 1}
		 * в обоих - вторичном и первичном индексах.
		 * Чтобы обработать эти конфликты, мы должны просканировать
		 * цепочки в направлении верхушки и проверить все insert стейтменты.
		 */
		struct memtx_story *newer_story = story;
		while (newer_story->link[i].newer_story != NULL) {
			newer_story = newer_story->link[i].newer_story;
			struct txn_stmt *test_stmt = newer_story->add_stmt;
			/* Не конфликтуем с собственными изменениями. */
			if (test_stmt->txn == stmt->txn)
				continue;
			/* Вставка после удаления этой же транзакцией. */
			if (test_stmt->is_own_change &&
			    test_stmt->del_story == NULL)
				continue;
			/* Если стейтмент перезаписывает нас в первичном индексе, то все ок. */
			if (test_stmt->del_story == story)
				continue;
			/*
			 * По вторичному индексу транзакция перезаписала нашу story,
			 * которую мы сейчас препейрим, но удалила другую. Пока не
			 * понятно, почему мы не отправили сразу в read view. Видимо,
			 * эта ситуация может меняться со временем.
			 */
			txn_send_to_read_view(test_stmt->txn, stmt->txn->psn);
		}
		/*
		 * Мы уже обработали gap readers для вставки в первичный индекс.
		 * В любом (replace или insert) случае мы должны обработать gap
		 * readers во вторичных индексах.
		 * Заметим, что newer_story уже верхушка, поэтому мы решили сделать
		 * странный интерфейс, хотя могли бы в memtx_tx_handle_conflict_gap_readers
		 * добавить одну проверку и не заплатили бы за нее ничего, зато был
		 * бы порядок.
		 */
		memtx_tx_handle_conflict_gap_readers(newer_story, i, stmt->txn);
	}

	/* Выставляем psn PSNs в stories, чтобы показать, что они - prepared. */
	stmt->add_story->add_psn = stmt->txn->psn;
	if (stmt->del_story != NULL)
		stmt->del_story->del_psn = stmt->txn->psn;
}

/**
 * Хелпер функции memtx_tx_history_prepare_stmt. Предусмотрен для случая
 * stmt->add_story == NULL, например, DELETE.
 */
static void
memtx_tx_history_prepare_delete_stmt(struct txn_stmt *stmt)
{
	assert(stmt->add_story == NULL);
	assert(stmt->del_story != NULL);

	/*
	 * Могут быть другие транзакции, которые хотели удалить old_story.
	 * Поскольку story становится prepared (old_story прекращает действовать),
	 * все они должны быть отсоединены от old_story.
	 */
	struct txn_stmt **from = &stmt->del_story->del_stmt;
	while (*from != NULL) {
		struct txn_stmt *test_stmt = *from;
		assert(test_stmt->del_story == stmt->del_story);
		if (test_stmt == stmt) {
			/* Отсоединяем всех кроме себя. */
			from = &test_stmt->next_in_del_list;
			continue;
		}
		assert(test_stmt->txn != stmt->txn);
		assert(test_stmt->del_story == stmt->del_story);
		assert(test_stmt->txn->psn == 0);

		/* Отсоединяем. */
		*from = test_stmt->next_in_del_list;
		test_stmt->next_in_del_list = NULL;
		test_stmt->del_story = NULL;
	}

	/*
	 * Story stmt->del_story перестает действовать. Каждая транзакция, которая
	 * зависит от нее, должна отправиться в read view либо заабортиться.
	 */
	memtx_tx_handle_conflict_story_readers(stmt->del_story, stmt->txn);

	/* Выставляем PSN в story, чтобы показать, что соотв. транзакция - prepared. */
	stmt->del_story->del_psn = stmt->txn->psn;
}

void
memtx_tx_history_prepare_stmt(struct txn_stmt *stmt)
{
	assert(stmt->txn->psn != 0);
	assert(stmt->space != NULL);
	/* ephemeral поддерживать не будем. */
	//if (stmt->space->def->opts.is_ephemeral)
	//	assert(stmt->add_story == NULL && stmt->del_story == NULL);

	/*
	 * Заметим, что оба add_story и del_story могут быть NULL в случаях:
	 * * Спейс - is_ephemeral (нам это пока не интересно).
	 * * Во время initial recovery (тоже скип).
	 * Для нас актуально:
	 * * Это удаление из спейса по ключу, который не был найден в спейсе.
	 * В каждом из этих случаев, ничего делать не требуется.
	 */
	if (stmt->add_story != NULL)
		memtx_tx_history_prepare_insert_stmt(stmt);
	else if (stmt->del_story != NULL)
		memtx_tx_history_prepare_delete_stmt(stmt);

	memtx_tx_story_gc();
}


void
memtx_tx_prepare_finalize_slow(struct txn *txn)
{
	memtx_tx_clear_txn_read_lists(txn);
}

void
memtx_tx_history_commit_stmt(struct txn_stmt *stmt)
{
	/* Статистики нам сейчас не интесны. */
	//tuple *old_tuple, *new_tuple;
	//old_tuple = stmt->del_story == NULL ? NULL : stmt->del_story->tuple;
	//new_tuple = stmt->add_story == NULL ? NULL : stmt->add_story->tuple;
	//memtx_space_update_tuple_stat(stmt->space, old_tuple, new_tuple);

	/*
	 * Отсоединяем add_story и del_story от стейтмента. Не понятно, почему это
	 * делается только на коммите, а не на prepare.
	 */
	if (stmt->add_story != NULL) {
		assert(stmt->add_story->add_stmt == stmt);
		memtx_tx_story_unlink_added_by(stmt->add_story, stmt);
	}
	if (stmt->del_story != NULL) {
		assert(stmt->del_story->del_stmt == stmt);
		memtx_tx_story_unlink_deleted_by(stmt->del_story, stmt);
	}
	memtx_tx_story_gc();
}

/* Хелпер функции @sa memtx_tx_tuple_clarify. */
static struct tuple *
memtx_tx_story_clarify_impl(struct txn *txn, struct memtx_space *space, struct memtx_story *top_story, struct index *index, /*uint32_t mk_index, */bool is_prepared_ok)
{
	struct memtx_story *story = top_story;
	bool own_change = false;
	struct tuple *result = NULL;

	while (true) {
		/* Удаление видимо. */
		if (memtx_tx_story_delete_is_visible(story, txn, is_prepared_ok, &own_change)) {
			result = NULL;
			break;
		}
		/*
		 * Удаление не видимо, но транзакция prepared, но не закоммичена.
		 * Если мы не видим её, значит должны быть сериализованы раньше,
		 * но мы не prepared => у нас два пути - быть read only, либо заабортиться.
		 */
		if (story->del_psn != 0 && story->del_stmt != NULL && txn != NULL) {
			assert(story->del_psn == story->del_stmt->txn->psn);
			txn_send_to_read_view(txn, story->del_stmt->txn->psn);
		}
		/* Добавление видимо. */
		if (memtx_tx_story_insert_is_visible(story, txn, is_prepared_ok, &own_change)) {
			result = story->tuple;
			break;
		}
		/* Аналогично. */
		if (story->add_psn != 0 && story->add_stmt != NULL && txn != NULL) {
			assert(story->add_psn == story->add_stmt->txn->psn);
			txn_send_to_read_view(txn, story->add_stmt->txn->psn);
		}

		/* Шаг назад. */
		if (story->link[index->dense_id].older_story == NULL)
			break;
		story = story->link[index->dense_id].older_story;
	}
	if (txn != NULL && !own_change) {
		/*
		 * Если результирующий тапл существует (видим) - он виден в каждом
		 * индексе. Но если мы нашли историю удаленного кортежа - мы должны
		 * записать, что только в данном индексе эта транзакция ничего не
		 * нашла по этому ключу.
		 * 
		 * Короче, для чтений важно гарантировать, что то, что мы прочитали,
		 * то и будет читаться в сериализованном порядке, поэтому мы трекаем здесь.
		 */
		if (result == NULL)
			memtx_tx_track_story_gap(txn, top_story, index->dense_id);
		else
			memtx_tx_track_read_story(txn, space, story);
	}
	return result;
}

/* Хелпер @sa memtx_tx_tuple_clarify. */
static struct tuple *
memtx_tx_tuple_clarify_impl(struct txn *txn, struct memtx_space *space, struct tuple *tuple, struct index *index, /*uint32_t mk_index, */bool is_prepared_ok)
{
	assert(tuple_has_flag(tuple, TUPLE_IS_DIRTY));
	struct memtx_story *story = memtx_tx_story_get(tuple);
	return memtx_tx_story_clarify_impl(txn, space, story, index, /*mk_index, */is_prepared_ok);
}

/**
 * Helper of @sa memtx_tx_tuple_clarify.
 * Detect whether the transaction can see prepared, but unconfirmed commits.
 */
//static bool
//detect_whether_prepared_ok(struct txn *txn, struct memtx_space *space)
//{
//	if (txn == NULL)
//		return false;
//	else if (txn->isolation == TXN_ISOLATION_READ_COMMITTED)
//		return true;
//	else if (txn->isolation == TXN_ISOLATION_READ_CONFIRMED ||
//		 txn->isolation == TXN_ISOLATION_LINEARIZABLE)
//		return false;
//	assert(txn->isolation == TXN_ISOLATION_BEST_EFFORT);
//	/*
//	 * The best effort that we can make is to determine whether the
//	 * transaction is read-only or not. For read only (including autocommit
//	 * select, that is txn == NULL) we should see only confirmed changes,
//	 * ignoring prepared. For read-write transaction we should see prepared
//	 * changes in order to avoid conflicts.
//	 */
//	return !stailq_empty(&txn->stmts);
//}

/**
 * Хелпер функции @sa memtx_tx_tuple_clarify.
 * Определяет is_prepared_ok флаг и отдает в memtx_tx_tuple_clarify_impl.
 */
struct tuple *
memtx_tx_tuple_clarify_slow(struct txn *txn, struct memtx_space *space, struct tuple *tuple, struct index *index/*, uint32_t mk_index*/)
{
	/* Если у тапла нет никакой истории, просто читаем его и, конечно, трекаем это чтение. */
	if (!tuple_has_flag(tuple, TUPLE_IS_DIRTY)) {
		memtx_tx_track_read(txn, space, tuple);
		return tuple;
	}
	//bool is_prepared_ok = detect_whether_prepared_ok(txn, space);
	bool is_prepared_ok = true;
	struct tuple *res = memtx_tx_tuple_clarify_impl(txn, space, tuple, index, /*mk_index, */is_prepared_ok);
	return res;
}

/**
 * Определяет, виден ли тапл @a tuple из индекса @a index спейса @a space
 * для транзакции @a txn.
 */
bool
memtx_tx_tuple_key_is_visible_slow(struct txn *txn, struct memtx_space *space, struct index *index, struct tuple *tuple)
{
	if (!tuple_has_flag(tuple, TUPLE_IS_DIRTY))
		return true;

	struct memtx_story *story = memtx_tx_story_get(tuple);
	struct tuple *visible = NULL;
	//bool is_prepared_ok = detect_whether_prepared_ok(txn, space);
	bool is_prepared_ok = true;
	bool unused;
	memtx_tx_story_find_visible_tuple(story, txn, index->dense_id, is_prepared_ok, &visible, &unused);
	return visible != NULL;
}

/**
 * Аллоцирует и инициализирует tx_read_tracker, возвращает NULL
 * в случае ошибки. Линки в спиках не инициализируются.
 */
static struct tx_read_tracker *
tx_read_tracker_new(struct txn *reader, struct memtx_story *story)
{
	struct tx_read_tracker *tracker = (struct tx_read_tracker *)malloc(sizeof(struct tx_read_tracker));
	tracker->reader = reader;
	tracker->story = story;
	return tracker;
}

/**
 * Трекает тот факт, что транзакция @a txn прочитала стори @a story в спейсе @a space.
 * Этот факт может привести к тому что транзакция отправится в read view или сконфликтует.
 * 
 * Изначально называлась memtx_tx_cause_conflict.
 */
static void
memtx_tx_track_read_story(struct txn *txn, struct memtx_space *space, struct memtx_story *story)
{
	/* ephemeral поддерживать не будем. */
	if (txn == NULL || space == NULL/* || space->def->opts.is_ephemeral*/)
		return;
	(void)space;
	assert(story != NULL);
	struct tx_read_tracker *tracker = NULL;

	/* Это тот самый странный цикл, который был еще в первых патчах. */
	struct rlist *r1 = story->reader_list.next;
	struct rlist *r2 = txn->read_set.next;
	/*
	 * Идем двумя указателями по списку reader_list (r1) в story и
	 * read_set в транзакции (r2). Двигаем их одновременно, пока один
	 * из них не укажет на то, что нам нужно (r1 на транзакцию или r2 на story).
	 */
	while (r1 != &story->reader_list && r2 != &txn->read_set) {
		tracker = rlist_entry(r1, struct tx_read_tracker, in_reader_list);
		assert(tracker->story == story);
		if (tracker->reader == txn)
			break;
		tracker = rlist_entry(r2, struct tx_read_tracker, in_read_set);
		assert(tracker->reader == txn);
		if (tracker->story == story)
			break;
		tracker = NULL;
		r1 = r1->next;
		r2 = r2->next;
	}
	/*
	 * Нашли трекер - перемещаем в начало. Видимо это должно как-то что-то
	 * соптимизировать, но пока не понятно что именно и как.
	 */
	if (tracker != NULL) {
		rlist_del(&tracker->in_reader_list);
		rlist_del(&tracker->in_read_set);
	} else {
		tracker = tx_read_tracker_new(txn, story);
	}
	/* Вот то место, где мы пушим в read_list. */
	rlist_add(&story->reader_list, &tracker->in_reader_list);
	rlist_add(&txn->read_set, &tracker->in_read_set);
}

/**
 * Трекает тот факт, что транзакция @a txn прочитала стори @a story в спейсе @a space.
 *
 * NB: can trigger story garbage collection.
 */
static void
memtx_tx_track_read(struct txn *txn, struct memtx_space *space, struct tuple *tuple)
{
	if (tuple == NULL)
		return;
	/* ephemeral поддерживать не будем. */
	if (txn == NULL || space == NULL/* || space->def->opts.is_ephemeral*/)
		return;

	if (tuple_has_flag(tuple, TUPLE_IS_DIRTY)) {
		struct memtx_story *story = memtx_tx_story_get(tuple);
		memtx_tx_track_read_story(txn, space, story);
	} else {
		/*
		 * Даже если тапл не DIRTY, создает для него story (тем самым делая его DIRTY),
		 * но не вызывает memtx_tx_track_read_story, вместо этого самостоятельно пушит
		 * трекер в списки, просто потому что здесь так можно сделать - story только
		 * что была создана, понятно что этого трекера там сейчас нет (там сейчас нет
		 * никакого трекера).
		 * 
		 * Мне кажется, могли бы просто позвать memtx_tx_track_read_story для красоты,
		 * один вызов и два лишних джампа ничего нам стоить не будут.
		 */
		struct memtx_story *story = memtx_tx_story_new(space, tuple);
		struct tx_read_tracker *tracker;
		tracker = tx_read_tracker_new(txn, story);
		rlist_add(&story->reader_list, &tracker->in_reader_list);
		rlist_add(&txn->read_set, &tracker->in_read_set);
	}
}

/**
 * Create new point_hole_item by given arguments and put it to hash table.
 * 
 * Вызывается только из memtx_tx_track_point_slow.
 */
static void
//point_hole_storage_new(index *index, const char *key, size_t key_len, txn *txn)
point_hole_storage_new(struct index *index, int key, struct txn *txn)
{
	//memtx_tx_mempool *pool = &txm.point_hole_item_pool;
	//point_hole_item *object = memtx_tx_xmempool_alloc(txn, pool);

	struct point_hole_item *object = point_hole_item_new();

	rlist_create(&object->ring);
	rlist_create(&object->in_point_holes_list);
	object->txn = txn;
	//index_ref(index);
	//if (key_len <= sizeof(object->short_key)) {
	//	object->key = object->short_key;
	//} else {
	//	object->key = memtx_tx_xregion_alloc(txn, key_len, MEMTX_TX_ALLOC_TRACKER);
	//}
	//memcpy((char *)object->key, key, key_len);
	object->key = key;
	object->is_head = true;

	key_def *def = &index->_key_def;
	uint32_t hash = /*key_hash(key, def)*/tuple_hash(key, def);
	object->hash = point_hole_storage_combine_index_and_tuple_hash(index, hash);

	const struct point_hole_item **put = (const struct point_hole_item **)&object;
	struct point_hole_item *replaced = NULL;
	struct point_hole_item **preplaced = &replaced;
	mh_point_holes_put(txm.point_holes, put, &preplaced, 0);
	if (preplaced != NULL) {
		/*
		 * The item in hash table was overwitten. It's OK, but
		 * we need replaced item to the item list.
		 */
		rlist_add(&replaced->ring, &object->ring);
		assert(replaced->is_head);
		replaced->is_head = false;
	}
	rlist_add(&txn->point_holes_list, &object->in_point_holes_list);
}

static void
point_hole_storage_delete(struct point_hole_item *object)
{
	if (!object->is_head) {
		assert(!rlist_empty(&object->ring));
	} else if (!rlist_empty(&object->ring)) {
		/*
		 * Hash table point to this item, but there are more
		 * items in the list. Relink the hash table with any other
		 * item in the list, and delete this item from the list.
		 */
		struct point_hole_item *another = rlist_next_entry(object, ring);

		const struct point_hole_item **put = (const struct point_hole_item **) &another;
		struct point_hole_item *replaced = NULL;
		struct point_hole_item **preplaced = &replaced;
		mh_point_holes_put(txm.point_holes, put, &preplaced, 0);
		assert(replaced == object);
		rlist_del(&object->ring);
		another->is_head = true;
	} else {
		/*
		 * Hash table point to this item, and it's the last in the
		 * list. We have to remove the item from the hash table.
		 */
		const struct point_hole_item **key = (const struct point_hole_item **)&object;
		mh_int_t pos = mh_point_holes_get(txm.point_holes, key, 0);
		assert(pos != mh_end(txm.point_holes));
		mh_point_holes_del(txm.point_holes, pos, 0);
	}
	point_hole_item_delete(object);
}

/**
 * Запомнить тот факт, что транзакция @a txn не прочитана ничего
 * в спейсе @a space в индексе @a index. Вызывается из memtx_tx_track_point.
 */
void
memtx_tx_track_point_slow(struct txn *txn, struct index *index, int key)
{
	if (txn->status != TXN_INPROGRESS)
		return;

	//key_def *def = index->def->_key_def;
	//const char *tmp = key;
	//for (uint32_t i = 0; i < def->part_count; i++)
	//	mp_next(&tmp);
	//size_t key_len = tmp - key;
	point_hole_storage_new(index, key, txn);
}

/* Clean and clear all read lists of @a txn. */
static void
memtx_tx_clear_txn_read_lists(struct txn *txn)
{
	while (!rlist_empty(&txn->point_holes_list)) {
		struct point_hole_item *object = rlist_first_entry(&txn->point_holes_list, struct point_hole_item, in_point_holes_list);
		point_hole_storage_delete(object);
	}
	while (!rlist_empty(&txn->gap_list)) {
		struct inplace_gap_item *item = rlist_first_entry(&txn->gap_list, struct inplace_gap_item, in_gap_list);
		memtx_tx_inplace_gap_item_delete(item);
	}

	struct tx_read_tracker *tracker, *tmp;
	rlist_foreach_entry_safe(tracker, &txn->read_set, in_read_set, tmp) {
		rlist_del(&tracker->in_reader_list);
		rlist_del(&tracker->in_read_set);
	}
	assert(rlist_empty(&txn->read_set));

	rlist_del(&txn->in_read_view_txns);
}

/* Clean memtx_tx part of @a txn. */
void
memtx_tx_clean_txn(struct txn *txn)
{
	memtx_tx_clear_txn_read_lists(txn);

	memtx_tx_story_gc();
}

void
memtx_tx_manager_init(void)
{
	rlist_create(&txm.read_view_txns);
	txm.history = mh_history_new();

	txm.point_holes = mh_point_holes_new();
	rlist_create(&txm.all_stories);
	txm.traverse_all_stories = &txm.all_stories;
	txm.must_do_gc_steps = 0;
}

void
memtx_tx_manager_free(void)
{
	struct txn *txn;
	rlist_foreach_entry(txn, &txns, in_txns)
		memtx_tx_clear_txn_read_lists(txn);

	struct memtx_story *story, *tmp;
	rlist_foreach_entry_safe(story, &txm.all_stories, in_all_stories, tmp) {
		for (size_t i = 0; i < story->index_count; i++)
			story->link[i].in_index = NULL;
		memtx_tx_story_full_unlink_on_space_delete(story);
		memtx_tx_story_delete(story);
	}
	mh_history_delete(txm.history);
	mh_point_holes_delete(txm.point_holes);
}
