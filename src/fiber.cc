// coro_meta.cc
#include "fiber.h"

// Определение глобальной переменной
thread_local Task* current_task = nullptr;

// Реализация методов Task
Task Task::promise_type::get_return_object() { 
    return Task{this}; 
}

std::suspend_always Task::promise_type::initial_suspend() { 
    fiber = {.id = 1, .name = "fiber", .txn = nullptr};
    return {}; 
}

std::suspend_always Task::promise_type::final_suspend() noexcept { 
    return {}; 
}

void Task::promise_type::return_void() {}
void Task::promise_type::unhandled_exception() { std::terminate(); }

struct fiber* Task::fiber() { 
    return &promise->fiber; 
}

// Реализация C-доступа
extern "C" struct fiber *fiber() {
    return current_task ? current_task->fiber() : nullptr;
}
