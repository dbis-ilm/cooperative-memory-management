#pragma once

#include <stddef.h>

#include "../../core/units.hpp"

struct TableBasepage {
    size_t cardinality;
    PageId primary_key_index_basepage;
    PageId column_basepages[];
};