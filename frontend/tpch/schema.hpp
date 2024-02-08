#pragma once

// LINEITEM
#define L_ORDERKEY_CID 0
#define L_PARTKEY_CID 1
#define L_SUPPKEY_CID 2
#define L_LINENUMBER_CID 3
#define L_QUANTITY_CID 4
#define L_EXTENDEDPRICE_CID 5
#define L_DISCOUNT_CID 6
#define L_TAX_CID 7
#define L_SHIPDATE_CID 8
#define L_COMMITDATE_CID 9
#define L_RECEIPTDATE_CID 10

// ORDERS
#define O_ORDERKEY_CID 0
#define O_CUSTKEY_CID 1
#define O_TOTALPRICE_CID 2
#define O_ORDERDATE_CID 3
#define O_SHIPPRIORITY_CID 4

// PARTSUPP
#define PS_PARTKEY_CID 0
#define PS_SUPPKEY_CID 1
#define PS_AVAILQTY_CID 2
#define PS_SUPPLYCOST_CID 3

// PART
#define P_PARTKEY_CID 0
#define P_SIZE_CID 1
#define P_RETAILPRICE_CID 2

// CUSTOMER
#define C_CUSTKEY_CID 0
#define C_NATIONKEY_CID 1
#define C_ACCTBAL_CID 2

// SUPPLIER
#define S_SUPPKEY_CID 0
#define S_NATIONKEY_CID 1
#define S_ACCTBAL_CID 2

// NATION
#define N_NATIONKEY_CID 0
#define N_REGIONKEY_CID 1

// REGION
#define R_REGIONKEY_CID 0


// NamedColumn declarations
extern NamedColumn n_nationkey;
extern NamedColumn n_regionkey;

extern NamedColumn ps_partkey;
extern NamedColumn ps_suppkey;
extern NamedColumn ps_availqty;
extern NamedColumn ps_supplycost;

extern NamedColumn s_nationkey;
extern NamedColumn s_suppkey;

extern NamedColumn p_partkey;
extern NamedColumn p_size;

extern NamedColumn l_suppkey;
extern NamedColumn l_partkey;
extern NamedColumn l_orderkey;
extern NamedColumn l_extendedprice;
extern NamedColumn l_quantity;
extern NamedColumn l_discount;
extern NamedColumn l_shipdate;

extern NamedColumn o_orderkey;