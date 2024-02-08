#pragma once

#include <stdint.h>

#include "../../core/units.hpp"

#define ROOTPAGE_MAGIC 0xfedcba9876543210ull
#define PERSISTENCE_VERSION 4ull

struct RootPage {
    uint64_t magic;
    uint64_t persistence_version;
    PageId schema_catalog_basepage;
    PageId table_catalog_basepage;
    PageId column_catalog_basepage;
};