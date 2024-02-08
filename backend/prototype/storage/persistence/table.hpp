#pragma once

#include <stddef.h>

#include "../../core/units.hpp"

typedef uint64_t RowId;

struct TableBasepage {
    size_t _reserved; // this used to be 'cardinality', keeping it reserved to avoid a backward-incompatible persistence change
    PageId visibility_basepage; // root page for the B+-Tree containing visibility information for this relation
    PageId primary_key_index_basepage;
    PageId additional_index_basepage; // currently only used for the O_D_ID, O_W_ID, O_C_ID, and O_ID index on ORDER
    PageId column_basepages[];
};