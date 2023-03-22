project(prototype_test)

set(CMAKE_CXX_STANDARD 14)

include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG release-1.12.1
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

file(GLOB_RECURSE PROTOTYPE_TESTS_CC **.cpp **/**.cpp **.hpp **/**.hpp)
add_executable(prototype_test ${PROTOTYPE_TESTS_CC})
target_include_directories(prototype_test PRIVATE ${CMAKE_SOURCE_DIR})
set_target_properties(prototype_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
target_link_libraries(prototype_test prototype GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(prototype_test WORKING_DIRECTORY ${CMAKE_BINARY_DIR})