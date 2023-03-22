#include <gtest/gtest.h>

#include "prototype/utils/CSV.hpp"

TEST(CSV, parse_decimal) {
    const char c1[] = "1341.31";
    EXPECT_EQ(parse_decimal(c1, sizeof(c1) - 1, 2), 134131);
    const char c2[] = "1341.31";
    EXPECT_EQ(parse_decimal(c2, sizeof(c2) - 1, 4), 13413100);
    const char c3[] = "-1341.31";
    EXPECT_EQ(parse_decimal(c3, sizeof(c3) - 1, 4), -13413100);
    const char c4[] = "30000.00";
    EXPECT_EQ(parse_decimal(c4, sizeof(c4) - 1, 2), 3000000);
}

TEST(CSV, parse_date) {
    const char c1[] = "2023-01-27";
    EXPECT_EQ(parse_date(c1, sizeof(c1) - 1), encode_date(2023, 1, 27));
}

TEST(CSV, parse_date_time) {
    const char c1[] = "2023-01-27 15:50:02";
    EXPECT_EQ(parse_date_time(c1, sizeof(c1) - 1), encode_date_time(2023, 1, 27, 15, 50, 2));
}

TEST(CSV, parse_csv) {
    // from TPC-H SF 10 ORDERS
    const char* input_str =
        "1|369001|O|186600.18|1996-01-02|5-LOW|Clerk#000009506|0|nstructions sleep furiously among |\n"
        "2|780017|O|66219.63|1996-12-01|1-URGENT|Clerk#000008792|0| foxes. pending accounts at the pending, silent asymptot|\n"
        "3|1233140|F|270741.97|1993-10-14|5-LOW|Clerk#000009543|0|sly final accounts boost. carefully regular ideas cajole carefully. depos|\n"
        "4|1367761|O|41714.38|1995-10-11|5-LOW|Clerk#000001234|0|sits. slyly regular warthogs cajole. regular, regular theodolites acro|\n"
        "5|444848|F|122444.33|1994-07-30|5-LOW|Clerk#000009248|0|quickly. bold deposits sleep slyly. packages use slyly|\n";
    std::stringstream input(input_str);
    std::vector<ParseTypeDescription> types;
    types.push_back(ParseTypeDescription::Int32());     // o_orderkey
    types.push_back(ParseTypeDescription::Int32());     // o_custkey
    types.push_back(ParseTypeDescription::Skip());      // o_orderstatus
    types.push_back(ParseTypeDescription::Decimal(2));  // o_totalprice
    types.push_back(ParseTypeDescription::Date());      // o_orderdate
    types.push_back(ParseTypeDescription::Skip());      // o_orderpriority
    types.push_back(ParseTypeDescription::Char(15));    // o_clerk
    types.push_back(ParseTypeDescription::Int32());     // o_shippriority
    types.push_back(ParseTypeDescription::Skip());      // o_comment
    std::vector<uint32_t> o_orderkey;
    std::vector<uint32_t> o_custkey;
    std::vector<uint64_t> o_totalprice;
    std::vector<uint32_t> o_orderdate;
    std::vector<char> o_clerk;
    std::vector<uint32_t> o_shippriority;
    std::vector<void*> destinations;
    destinations.push_back(&o_orderkey);
    destinations.push_back(&o_custkey);
    destinations.push_back(nullptr);
    destinations.push_back(&o_totalprice);
    destinations.push_back(&o_orderdate);
    destinations.push_back(nullptr);
    destinations.push_back(&o_clerk);
    destinations.push_back(&o_shippriority);
    destinations.push_back(nullptr);
    EXPECT_EQ(parse_csv_chunk(input, 0, strlen(input_str), '|', types, destinations), 5);
    EXPECT_EQ(o_orderkey, std::vector<uint32_t>({ 1, 2, 3, 4, 5 }));
    EXPECT_EQ(o_custkey, std::vector<uint32_t>({ 369001, 780017, 1233140, 1367761, 444848 }));
    EXPECT_EQ(o_totalprice, std::vector<uint64_t>({ 18660018, 6621963, 27074197, 4171438, 12244433 }));
    EXPECT_EQ(o_orderdate, std::vector<uint32_t>({ encode_date(1996, 01, 02), encode_date(1996, 12, 01), encode_date(1993, 10, 14), encode_date(1995, 10, 11), encode_date(1994, 07, 30) }));
    EXPECT_EQ(o_clerk.size(), 15 * 5);
    o_clerk.push_back(0);
    EXPECT_STREQ(o_clerk.data(), "Clerk#000009506Clerk#000008792Clerk#000009543Clerk#000001234Clerk#000009248");
    EXPECT_EQ(o_shippriority, std::vector<uint32_t>({ 0, 0, 0, 0, 0 }));
}

TEST(CSV, parse_csv_chunk_lineskip) {
    // from TPC-CH 10 WH NEWORDER
    const char* input_str =
        "2779|1|1\n"
        "2698|1|1\n";
    std::vector<ParseTypeDescription> types;
    types.push_back(ParseTypeDescription::Int32()); // NO_O_ID_CID
    types.push_back(ParseTypeDescription::Int32()); // NO_D_ID_CID
    types.push_back(ParseTypeDescription::Int32()); // NO_W_ID_CID
    std::vector<uint32_t> no_o_id;
    std::vector<uint32_t> no_d_id;
    std::vector<uint64_t> no_w_id;
    std::vector<void*> destinations;
    destinations.push_back(&no_o_id);
    destinations.push_back(&no_d_id);
    destinations.push_back(&no_w_id);
    std::stringstream input(input_str);
    EXPECT_EQ(parse_csv_chunk(input, 0, strlen(input_str), '|', types, destinations), 2);

    for (size_t i = 0; i < 18; i++) {
        size_t first_chunk_row_count = i == 0 ? 0 : (i <= 9 ? 1 : 2);
        input = std::stringstream(input_str);
        EXPECT_EQ(parse_csv_chunk(input, 0, i, '|', types, destinations), first_chunk_row_count);
        input = std::stringstream(input_str);
        EXPECT_EQ(parse_csv_chunk(input, i, strlen(input_str + i), '|', types, destinations), 2 - first_chunk_row_count);
    }
}