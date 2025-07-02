#pragma once

// coro_meta.h (совместим с C и C++)
#ifdef __cplusplus
#include <coroutine>
#include <cstdint>
#include <exception>

// Структура с метаданными корутины (видима в C)
struct fiber {
    uint64_t id;          // ID корутины
    const char* name;     // Имя (для дебага)
    struct txn *txn;      // Пользовательские данные (можно модифицировать из C)
};

// Объявление функции (реализация в .cc)
extern "C" struct fiber *fiber();

// Корутина с метаданными
struct Task {
    struct promise_type {
        struct fiber fiber;  // Метаданные, доступные из C

        Task get_return_object();
        std::suspend_always initial_suspend();
        std::suspend_always final_suspend() noexcept;
        void return_void();
        void unhandled_exception();
    };

    promise_type* promise;

    // Возвращает метаданные из C
    struct fiber* fiber();
};

// Объявление глобальной переменной (определение в .cc)
extern thread_local Task* current_task;

#else // Чистый C
struct fiber {
    uint64_t id;
    const char* name;
    void* txn;
};

struct fiber *fiber();
#endif
