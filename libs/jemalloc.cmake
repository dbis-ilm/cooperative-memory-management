# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

# Get jemalloc
ExternalProject_Add(
    jemalloc_src
    PREFIX "vendor/jemalloc"
    URL https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2
    URL_HASH SHA256=2db82d1e7119df3e71b7640219b6dfe84789bc0537983c3b7ac4f7189aecfeaa
    BUILD_IN_SOURCE TRUE
    CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER}
    BUILD_COMMAND make build_lib install_lib install_include
)

# Prepare jemalloc
ExternalProject_Get_Property(jemalloc_src install_dir)
set(JEMALLOC_INCLUDE_DIR ${install_dir}/include)
set(JEMALLOC_LIBRARY_PATH ${install_dir}/lib/libjemalloc.a)
file(MAKE_DIRECTORY ${JEMALLOC_INCLUDE_DIR})
add_library(jemalloc STATIC IMPORTED)
set_property(TARGET jemalloc PROPERTY IMPORTED_LOCATION ${JEMALLOC_LIBRARY_PATH})
set_property(TARGET jemalloc APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${JEMALLOC_INCLUDE_DIR})

# Dependencies
add_dependencies(jemalloc jemalloc_src)
