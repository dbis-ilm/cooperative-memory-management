project(prototype_test)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

file(GLOB_RECURSE PROTOTYPE_TESTS_CC **.cpp **/**.cpp **.hpp **/**.hpp ${CMAKE_SOURCE_DIR}/test/shared**/**.hpp ${CMAKE_SOURCE_DIR}/test/shared**/**.cpp)
add_executable(prototype_test ${PROTOTYPE_TESTS_CC})
target_include_directories(prototype_test PRIVATE ${CMAKE_SOURCE_DIR})
set_target_properties(prototype_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
target_link_libraries(prototype_test prototype GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(prototype_test WORKING_DIRECTORY ${CMAKE_BINARY_DIR})