include(ExternalProject)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

foreach (BENCHMARK tpch tpcch)
    file(GLOB_RECURSE LIB_CC ${BENCHMARK}/**.cpp ${BENCHMARK}/**/**.cpp ${BENCHMARK}/**.hpp ${BENCHMARK}/**/**.hpp)
    list(FILTER LIB_CC EXCLUDE REGEX ".*/${BENCHMARK}/main.cpp")

    add_library(${BENCHMARK}_lib STATIC ${LIB_CC})
    target_link_libraries(${BENCHMARK}_lib prototype)
    add_executable(${BENCHMARK} ${BENCHMARK}/main.cpp)
    target_link_libraries(${BENCHMARK} ${BENCHMARK}_lib gflags jemalloc dl)

    if (VTUNE_PROFILING)
        set(VTUNE_PATH "/opt/intel/oneapi/vtune/latest")
        target_link_libraries(${BENCHMARK} pthread dl ${VTUNE_PATH}/sdk/lib64/libittnotify.a)
        target_include_directories(${BENCHMARK} PRIVATE ${VTUNE_PATH}/sdk/include)
        target_compile_definitions(${BENCHMARK} PRIVATE VTUNE_PROFILING)
    endif()
endforeach()

ExternalProject_Add(
    chbenchmark
    PREFIX chbenchmark
    URL https://db.in.tum.de/research/projects/CHbenCHmark/chBenchmark_1_0.tar.bz2
    URL_HASH SHA512=d7637eac1044dd8723fbeee5e78677ef3c3dd557ee5710c490baa44ac42f2b0d30b89ae90a876b9783c7ff42bf09e00367ea9975b44d47838629f8cf7a3b3b8f
    CONFIGURE_COMMAND ""
    PATCH_COMMAND "${CMAKE_SOURCE_DIR}/scripts/ch-benchmark-patch.sh"
    BUILD_COMMAND "make"
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND ""
)

set(CH_DATA_DIR ${CMAKE_SOURCE_DIR}/data/tpcch)

foreach (NUM_WAREHOUSES 1 10 100)
    set(CH_WH_DATA_FILES ${CH_DATA_DIR}/${NUM_WAREHOUSES}/CUSTOMER.tbl ${CH_DATA_DIR}/${NUM_WAREHOUSES}/DISTRICT.tbl ${CH_DATA_DIR}/${NUM_WAREHOUSES}/HISTORY.tbl ${CH_DATA_DIR}/${NUM_WAREHOUSES}/ITEM.tbl ${CH_DATA_DIR}/${NUM_WAREHOUSES}/NATION.tbl ${CH_DATA_DIR}/${NUM_WAREHOUSES}/NEWORDER.tbl ${CH_DATA_DIR}/${NUM_WAREHOUSES}/ORDERLINE.tbl ${CH_DATA_DIR}/${NUM_WAREHOUSES}/ORDER.tbl ${CH_DATA_DIR}/${NUM_WAREHOUSES}/REGION.tbl ${CH_DATA_DIR}/${NUM_WAREHOUSES}/STOCK.tbl ${CH_DATA_DIR}/${NUM_WAREHOUSES}/SUPPLIER.tbl ${CH_DATA_DIR}/${NUM_WAREHOUSES}/WAREHOUSE.tbl)
    add_custom_command(
        OUTPUT ${CH_WH_DATA_FILES}
        COMMAND ${CMAKE_SOURCE_DIR}/scripts/ch-benchmark-dbgen.sh ${CH_DATA_DIR} ${NUM_WAREHOUSES}
        DEPENDS chbenchmark
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/frontend/chbenchmark/src/chbenchmark
        VERBATIM)
    list(APPEND CH_DATA_FILES ${CH_WH_DATA_FILES})
endforeach()

add_custom_target(tpcch_data ALL DEPENDS ${CH_DATA_FILES})