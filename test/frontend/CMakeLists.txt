project(frontend_test)

set(CMAKE_CXX_STANDARD 17)

file(GLOB_RECURSE FRONTEND_TESTS_CC **.cpp **/**.cpp **.hpp **/**.hpp ${CMAKE_SOURCE_DIR}/test/shared**/**.hpp ${CMAKE_SOURCE_DIR}/test/shared**/**.cpp)
add_executable(frontend_test ${FRONTEND_TESTS_CC})
target_include_directories(frontend_test PRIVATE ${CMAKE_SOURCE_DIR})
set_target_properties(frontend_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
target_link_libraries(frontend_test tpcch_lib GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(frontend_test WORKING_DIRECTORY ${CMAKE_BINARY_DIR})