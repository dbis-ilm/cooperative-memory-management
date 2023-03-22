# ---------------------------------------------------------------------------
# prototype
# ---------------------------------------------------------------------------
project(prototype)

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------
file(GLOB_RECURSE PROTOTYPE_CC **.cpp **/**.cpp **.hpp **/**.hpp)
if (NOT UNIX)
    message(SEND_ERROR "unsupported platform")
endif ()

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------
add_library(prototype STATIC ${PROTOTYPE_CC})

target_link_libraries(prototype gflags numa)

if (VTUNE_PROFILING)
    set(VTUNE_PATH "/opt/intel/oneapi/vtune/latest")
    target_link_libraries(prototype pthread dl ${VTUNE_PATH}/sdk/lib64/libittnotify.a)
    target_include_directories(prototype PRIVATE ${VTUNE_PATH}/sdk/include)
    target_compile_definitions(prototype PRIVATE VTUNE_PROFILING)
endif()


if (COLLECT_CACHE_TRACES)
    target_compile_definitions(prototype PRIVATE COLLECT_CACHE_TRACES)
endif()

set(PROTOTYPE_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR})
set_property(TARGET prototype APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${PROTOTYPE_INCLUDE_DIR})