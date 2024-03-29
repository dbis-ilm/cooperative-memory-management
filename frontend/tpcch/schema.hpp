#pragma once

// WAREHOUSE
// W_ID INTEGER (PRIMARY KEY)
#define W_ID_CID 0
// W_NAME CHAR(10)
#define W_NAME_CID 1
// W_STREET_1 CHAR(20)
#define W_STREET_1_CID 2
// W_STREET_2 CHAR(20)
#define W_STREET_2_CID 3
// W_CITY CHAR(20)
#define W_CITY_CID 4
// W_STATE CHAR(2)
#define W_STATE_CID 5
// W_ZIP CHAR(9)
#define W_ZIP_CID 6
// W_TAX DECIMAL(4,4)
#define W_TAX_CID 7
// W_YTD DECIMAL(12,2)
#define W_YTD_CID 8

// DISTRICT
// D_ID TINYINT (Composite PRIMARY KEY)
#define D_ID_CID 0
// D_W_ID INTEGER (Composite PRIMARY KEY)
#define D_W_ID_CID 1
// D_NAME CHAR(10)
#define D_NAME_CID 2
// D_STREET_1 CHAR(20)
#define D_STREET_1_CID 3
// D_STREET_2 CHAR(20)
#define D_STREET_2_CID 4
// D_CITY CHAR(20)
#define D_CITY_CID 5
// D_STATE CHAR(2)
#define D_STATE_CID 6
// D_ZIP CHAR(9)
#define D_ZIP_CID 7
// D_TAX DECIMAL(4,4)
#define D_TAX_CID 8
// D_YTD DECIMAL(12,2)
#define D_YTD_CID 9
// D_NEXT_O_ID INTEGER
#define D_NEXT_O_ID_CID 10

// CUSTOMER
// C_D_ID TINYINT (Composite PRIMARY KEY)
#define C_D_ID_CID 0
// C_W_ID INTEGER (Composite PRIMARY KEY)
#define C_W_ID_CID 1
// C_ID SMALLINT (Composite PRIMARY KEY)
#define C_ID_CID 2
// C_FIRST CHAR(16)
#define C_FIRST_CID 3
// C_MIDDLE CHAR(2)
#define C_MIDDLE_CID 4
// C_LAST CHAR(16)
#define C_LAST_CID 5
// C_STREET_1 CHAR(20)
#define C_STREET_1_CID 6
// C_STREET_2 CHAR(20)
#define C_STREET_2_CID 7
// C_CITY CHAR(20)
#define C_CITY_CID 8
// C_STATE CHAR(2)
#define C_STATE_CID 9
// C_ZIP CHAR(9)
#define C_ZIP_CID 10
// C_PHONE CHAR(16)
#define C_PHONE_CID 11
// C_SINCE SECONDDATE
#define C_SINCE_CID 12
// C_CREDIT CHAR(2)
#define C_CREDIT_CID 13
// C_CREDIT_LIM DECIMAL(12,2)
#define C_CREDIT_LIM_CID 14
// C_DISCOUNT DECIMAL(4,4)
#define C_DISCOUNT_CID 15
// C_BALANCE DECIMAL(12,2)
#define C_BALANCE_CID 16
// C_YTD_PAYMENT DECIMAL(12,2)
#define C_YTD_PAYMENT_CID 17
// C_PAYMENT_CNT SMALLINT
#define C_PAYMENT_CNT_CID 18
// C_DELIVERY_CNT SMALLINT
#define C_DELIVERY_CNT_CID 19
// C_DATA CHAR(500)
#define C_DATA_CID 20
// C_N_NATIONKEY INTEGER
#define C_N_NATIONKEY_CID 21

// HISTORY
// H_C_ID SMALLINT
#define H_C_ID_CID 0
// H_C_D_ID TINYINT
#define H_C_D_ID_CID 1
// H_C_W_ID INTEGER
#define H_C_W_ID_CID 2
// H_D_ID TINYINT
#define H_D_ID_CID 3
// H_W_ID INTEGER
#define H_W_ID_CID 4
// H_DATE SECONDDATE
#define H_DATE_CID 5
// H_AMOUNT DECIMAL(6,2)
#define H_AMOUNT_CID 6
// H_DATA CHAR(24)
#define H_DATA_CID 7

// NEWORDER
// NO_D_ID TINYINT (Composite PRIMARY KEY)
#define NO_D_ID_CID 0
// NO_W_ID INTEGER (Composite PRIMARY KEY)
#define NO_W_ID_CID 1
// NO_O_ID INTEGER (Composite PRIMARY KEY)
#define NO_O_ID_CID 2

// ORDER
// O_D_ID TINYINT (Composite PRIMARY KEY)
#define O_D_ID_CID 0
// O_W_ID INTEGER (Composite PRIMARY KEY)
#define O_W_ID_CID 1
// O_ID INTEGER (Composite PRIMARY KEY)
#define O_ID_CID 2
// O_C_ID SMALLINT
#define O_C_ID_CID 3
// O_ENTRY_D SECONDDATE
#define O_ENTRY_D_CID 4
// O_CARRIER_ID TINYINT
#define O_CARRIER_ID_CID 5
// O_OL_CNT TINYINT
#define O_OL_CNT_CID 6
// O_ALL_LOCAL TINYINT
#define O_ALL_LOCAL_CID 7

// ORDERLINE
// OL_D_ID TINYINT (Composite PRIMARY KEY)
#define OL_D_ID_CID 0
// OL_W_ID INTEGER (Composite PRIMARY KEY)
#define OL_W_ID_CID 1
// OL_O_ID INTEGER (Composite PRIMARY KEY)
#define OL_O_ID_CID 2
// OL_NUMBER TINYINT (Composite PRIMARY KEY)
#define OL_NUMBER_CID 3
// OL_I_ID INTEGER
#define OL_I_ID_CID 4
// OL_SUPPLY_W_ID INTEGER
#define OL_SUPPLY_W_ID_CID 5
// OL_DELIVERY_D SECONDDATE
#define OL_DELIVERY_D_CID 6
// OL_QUANTITY SMALLINT
#define OL_QUANTITY_CID 7
// OL_AMOUNT DECIMAL(6,2)
#define OL_AMOUNT_CID 8
// OL_DIST_INFO CHAR(24)
#define OL_DIST_INFO_CID 9

// ITEM
// I_ID INTEGER (PRIMARY KEY)
#define I_ID_CID 0
// I_IM_ID SMALLINT
#define I_IM_ID_CID 1
// I_NAME CHAR(24)
#define I_NAME_CID 2
// I_PRICE DECIMAL(5,2)
#define I_PRICE_CID 3
// I_DATA CHAR(50)
#define I_DATA_CID 4

// STOCK
// S_I_ID INTEGER (Composite PRIMARY KEY)
#define S_I_ID_CID 0
// S_W_ID INTEGER (Composite PRIMARY KEY)
#define S_W_ID_CID 1
// S_QUANTITY SMALLINT
#define S_QUANTITY_CID 2
// S_DIST_01 CHAR(24)
#define S_DIST_01_CID 3
// S_DIST_02 CHAR(24)
#define S_DIST_02_CID 4
// S_DIST_03 CHAR(24)
#define S_DIST_03_CID 5
// S_DIST_04 CHAR(24)
#define S_DIST_04_CID 6
// S_DIST_05 CHAR(24)
#define S_DIST_05_CID 7
// S_DIST_06 CHAR(24)
#define S_DIST_06_CID 8
// S_DIST_07 CHAR(24)
#define S_DIST_07_CID 9
// S_DIST_08 CHAR(24)
#define S_DIST_08_CID 10
// S_DIST_09 CHAR(24)
#define S_DIST_09_CID 11
// S_DIST_10 CHAR(24)
#define S_DIST_10_CID 12
// S_YTD INTEGER
#define S_YTD_CID 13
// S_ORDER_CNT SMALLINT
#define S_ORDER_CNT_CID 14
// S_REMOTE_CNT SMALLINT
#define S_REMOTE_CNT_CID 15
// S_DATA CHAR(50)
#define S_DATA_CID 16
// S_SU_SUPPKEY INTEGER
#define S_SU_SUPPKEY_CID 17


// NATION
// N_NATIONKEY TINYINT (PRIMARY KEY)
#define N_NATIONKEY_CID 0
// N_NAME CHAR(25)
#define N_NAME_CID 1
// N_REGIONKEY TINYINT
#define N_REGIONKEY_CID 2
// N_COMMENT CHAR(152)
#define N_COMMENT_CID 3

// SUPPLIER
// SU_SUPPKEY SMALLINT (PRIMARY KEY)
#define SU_SUPPKEY_CID 0
// SU_NAME CHAR(25)
#define SU_NAME_CID 1
// SU_ADDRESS CHAR(40)
#define SU_ADDRESS_CID 2
// SU_NATIONKEY TINYINT
#define SU_NATIONKEY_CID 3
// SU_PHONE CHAR(15)
#define SU_PHONE_CID 4
// SU_ACCTBAL DECIMAL(12,2)
#define SU_ACCTBAL_CID 5
// SU_COMMENT CHAR(101)
#define SU_COMMENT_CID 6

// REGION
// R_REGIONKEY TINYINT (PRIMARY KEY)
#define R_REGIONKEY_CID 0
// R_NAME CHAR(55)
#define R_NAME_CID 1
// R_COMMENT CHAR(152)
#define R_COMMENT_CID 2