#pragma once

#include "../../core/units.hpp"

struct ColumnBasepage {
    PageId next; // can point to another ColumnBasepage if a single page is too small to hold all data_pages
    PageId data_pages[];
};