cmake_minimum_required(VERSION 3.26)
project(memtx_tx C CXX ASM)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)

include(CheckLibraryExists)
include(CheckIncludeFile)
include(CheckCCompilerFlag)
include(CheckSymbolExists)
include(CheckCSourceRuns)
include(CheckCXXSourceRuns)
include(CheckCSourceCompiles)
include(CheckCXXSourceCompiles)
include(TestBigEndian)
include(CheckFunctionExists)
include(FindPackageMessage)
include(ExternalProject)

#
# Specify where to look for include files.
#
include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_BINARY_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/src/lib)
include_directories(${PROJECT_SOURCE_DIR}/src/lib/small/include)
include_directories(${PROJECT_BINARY_DIR}/src/lib/small/small/include)
include_directories(${PROJECT_SOURCE_DIR}/src/lib/small/third_party)
include_directories(${PROJECT_SOURCE_DIR}/src/lib/core)
include_directories(${PROJECT_SOURCE_DIR}/third_party)

include(cmake/utils.cmake)

check_symbol_exists(MAP_ANON sys/mman.h HAVE_MAP_ANON)
check_symbol_exists(MAP_ANONYMOUS sys/mman.h HAVE_MAP_ANONYMOUS)
check_symbol_exists(MADV_DONTNEED sys/mman.h HAVE_MADV_DONTNEED)
check_include_file(sys/time.h HAVE_SYS_TIME_H)
check_include_file(cpuid.h HAVE_CPUID_H)
check_include_file(sys/prctl.h HAVE_PRCTL_H)

check_symbol_exists(O_DSYNC fcntl.h HAVE_O_DSYNC)
check_symbol_exists(fdatasync unistd.h HAVE_FDATASYNC)
check_symbol_exists(pthread_yield pthread.h HAVE_PTHREAD_YIELD)
check_symbol_exists(sched_yield sched.h HAVE_SCHED_YIELD)
check_symbol_exists(posix_fadvise fcntl.h HAVE_POSIX_FADVISE)
check_symbol_exists(fallocate fcntl.h HAVE_FALLOCATE)
check_symbol_exists(mremap sys/mman.h HAVE_MREMAP)

check_function_exists(sync_file_range HAVE_SYNC_FILE_RANGE)
check_function_exists(memmem HAVE_MEMMEM)
check_function_exists(memrchr HAVE_MEMRCHR)
check_function_exists(sendfile HAVE_SENDFILE)
if (HAVE_SENDFILE)
    if (TARGET_OS_LINUX)
        set(HAVE_SENDFILE_LINUX 1)
    else(HAVE_SENDFILE)
        set(HAVE_SENDFILE_BSD 1)
    endif()
endif()
check_function_exists(uuidgen HAVE_UUIDGEN)
set(CMAKE_REQUIRED_LIBRARIES "")
if (TARGET_OS_LINUX)
    set(CMAKE_REQUIRED_LIBRARIES rt)
endif ()
check_symbol_exists(clock_gettime time.h HAVE_CLOCK_GETTIME_DECL)
if (HAVE_CLOCK_GETTIME_DECL AND TARGET_OS_DARWIN)
    # ensure clock_gettime() is declared and actually available
    # in runtime (gh-1777)
    check_c_source_runs(
        "#include <time.h>\nint main() {struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts); return 0;}"
        HAVE_CLOCK_GETTIME)
else ()
    set(HAVE_CLOCK_GETTIME ${HAVE_CLOCK_GETTIME_DECL})
endif ()

include_directories(${LIBEV_INCLUDE_DIR})

configure_file(
        "${PROJECT_SOURCE_DIR}/src/trivia/config.h.cmake"
        "${PROJECT_BINARY_DIR}/src/trivia/config.h"
)

enable_testing()

add_subdirectory(src)
#add_subdirectory(test)

set (sources
    src/box.c
    src/fiber.cc
    src/index.cc
    src/key_def.cc
    src/main.cc
    src/memtx_engine.c
    src/memtx_space.c
    src/memtx_tx.c
    src/txn.c
)

add_executable(memtx_tx ${sources})
add_dependencies(memtx_tx small salad)
target_link_libraries(memtx_tx small salad)
