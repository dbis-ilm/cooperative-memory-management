#pragma once

#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/execution/batch.hpp"
#include "prototype/execution/table_column.hpp"
#include "prototype/scheduling/execution_context.hpp"
#include "schema.hpp"

namespace tpcch {

// named column definitions for convenience
const auto NO_D_ID = NamedColumn(std::string("NO_D_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(NO_D_ID_CID));
const auto NO_W_ID = NamedColumn(std::string("NO_W_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(NO_W_ID_CID));
const auto NO_O_ID = NamedColumn(std::string("NO_O_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(NO_O_ID_CID));

const auto W_ID = NamedColumn(std::string("W_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(W_ID_CID));
const auto W_NAME = NamedColumn(std::string("W_NAME"), std::make_shared<UnencodedTableColumn<Char<10>>>(W_NAME_CID));
const auto W_STREET_1 = NamedColumn(std::string("W_STREET_1"), std::make_shared<UnencodedTableColumn<Char<20>>>(W_STREET_1_CID));
const auto W_STREET_2 = NamedColumn(std::string("W_STREET_2"), std::make_shared<UnencodedTableColumn<Char<20>>>(W_STREET_2_CID));
const auto W_CITY = NamedColumn(std::string("W_CITY"), std::make_shared<UnencodedTableColumn<Char<20>>>(W_CITY_CID));
const auto W_STATE = NamedColumn(std::string("W_STATE"), std::make_shared<UnencodedTableColumn<Char<2>>>(W_STATE_CID));
const auto W_ZIP = NamedColumn(std::string("W_ZIP"), std::make_shared<UnencodedTableColumn<Char<9>>>(W_ZIP_CID));
const auto W_TAX = NamedColumn(std::string("W_TAX"), std::make_shared<UnencodedTableColumn<Decimal<4>>>(W_TAX_CID));
const auto W_YTD = NamedColumn(std::string("W_YTD"), std::make_shared<UnencodedTableColumn<Decimal<2>>>(W_YTD_CID));

const auto D_NAME = NamedColumn(std::string("D_NAME"), std::make_shared<UnencodedTableColumn<Char<10>>>(D_NAME_CID));
const auto D_STREET_1 = NamedColumn(std::string("D_STREET_1"), std::make_shared<UnencodedTableColumn<Char<20>>>(D_STREET_1_CID));
const auto D_STREET_2 = NamedColumn(std::string("D_STREET_2"), std::make_shared<UnencodedTableColumn<Char<20>>>(D_STREET_2_CID));
const auto D_CITY = NamedColumn(std::string("D_CITY"), std::make_shared<UnencodedTableColumn<Char<20>>>(D_CITY_CID));
const auto D_STATE = NamedColumn(std::string("D_STATE"), std::make_shared<UnencodedTableColumn<Char<2>>>(D_STATE_CID));
const auto D_ZIP = NamedColumn(std::string("D_ZIP"), std::make_shared<UnencodedTableColumn<Char<9>>>(D_ZIP_CID));
const auto D_TAX = NamedColumn(std::string("D_TAX"), std::make_shared<UnencodedTableColumn<Decimal<4>>>(D_TAX_CID));
const auto D_YTD = NamedColumn(std::string("D_YTD"), std::make_shared<UnencodedTableColumn<Decimal<2>>>(D_YTD_CID));
const auto D_NEXT_O_ID = NamedColumn(std::string("D_NEXT_O_ID"), std::make_shared<UnencodedTableColumn<Integer>>(D_NEXT_O_ID_CID));

const auto C_ID = NamedColumn(std::string("C_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(C_ID_CID));
const auto C_FIRST = NamedColumn(std::string("C_FIRST"), std::make_shared<UnencodedTableColumn<Char<16>>>(C_FIRST_CID));
const auto C_MIDDLE = NamedColumn(std::string("C_MIDDLE"), std::make_shared<UnencodedTableColumn<Char<2>>>(C_MIDDLE_CID));
const auto C_LAST = NamedColumn(std::string("C_LAST"), std::make_shared<UnencodedTableColumn<Char<16>>>(C_LAST_CID));
const auto C_STREET_1 = NamedColumn(std::string("C_STREET_1"), std::make_shared<UnencodedTableColumn<Char<20>>>(C_STREET_1_CID));
const auto C_STREET_2 = NamedColumn(std::string("C_STREET_2"), std::make_shared<UnencodedTableColumn<Char<20>>>(C_STREET_2_CID));
const auto C_CITY = NamedColumn(std::string("C_CITY"), std::make_shared<UnencodedTableColumn<Char<20>>>(C_CITY_CID));
const auto C_STATE = NamedColumn(std::string("C_STATE"), std::make_shared<UnencodedTableColumn<Char<2>>>(C_STATE_CID));
const auto C_ZIP = NamedColumn(std::string("C_ZIP"), std::make_shared<UnencodedTableColumn<Char<9>>>(C_ZIP_CID));
const auto C_PHONE = NamedColumn(std::string("C_PHONE"), std::make_shared<UnencodedTableColumn<Char<16>>>(C_PHONE_CID));
const auto C_SINCE = NamedColumn(std::string("C_SINCE"), std::make_shared<UnencodedTableColumn<DateTime>>(C_SINCE_CID));
const auto C_CREDIT = NamedColumn(std::string("C_CREDIT"), std::make_shared<UnencodedTableColumn<Char<2>>>(C_CREDIT_CID));
const auto C_CREDIT_LIM = NamedColumn(std::string("C_CREDIT_LIM"), std::make_shared<UnencodedTableColumn<Decimal<2>>>(C_CREDIT_LIM_CID));
const auto C_DISCOUNT = NamedColumn(std::string("C_DISCOUNT"), std::make_shared<UnencodedTableColumn<Decimal<4>>>(C_DISCOUNT_CID));
const auto C_BALANCE = NamedColumn(std::string("C_BALANCE"), std::make_shared<UnencodedTableColumn<Decimal<2>>>(C_BALANCE_CID));
const auto C_YTD_PAYMENT = NamedColumn(std::string("C_YTD_PAYMENT"), std::make_shared<UnencodedTableColumn<Decimal<2>>>(C_YTD_PAYMENT_CID));
const auto C_PAYMENT_CNT = NamedColumn(std::string("C_PAYMENT_CNT"), std::make_shared<UnencodedTableColumn<Integer>>(C_PAYMENT_CNT_CID));
const auto C_DELIVERY_CNT = NamedColumn(std::string("C_DELIVERY_CNT"), std::make_shared<UnencodedTableColumn<Integer>>(C_DELIVERY_CNT_CID));
const auto C_DATA = NamedColumn(std::string("C_DATA"), std::make_shared<UnencodedTableColumn<Char<500>>>(C_DATA_CID));

const auto I_ID = NamedColumn(std::string("I_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(I_ID_CID));
const auto I_PRICE = NamedColumn(std::string("I_PRICE"), std::make_shared<UnencodedTableColumn<Decimal<2>>>(I_PRICE_CID));
const auto I_DATA = NamedColumn(std::string("I_DATA"), std::make_shared<UnencodedTableColumn<Char<50>>>(I_DATA_CID));

const auto S_QUANTITY = NamedColumn(std::string("S_QUANTITY"), std::make_shared<UnencodedTableColumn<Integer>>(S_QUANTITY_CID));
const auto S_YTD = NamedColumn(std::string("S_YTD"), std::make_shared<UnencodedTableColumn<Integer>>(S_YTD_CID));
const auto S_ORDER_CNT = NamedColumn(std::string("S_ORDER_CNT"), std::make_shared<UnencodedTableColumn<Integer>>(S_ORDER_CNT_CID));
const auto S_REMOTE_CNT = NamedColumn(std::string("S_REMOTE_CNT"), std::make_shared<UnencodedTableColumn<Integer>>(S_REMOTE_CNT_CID));

const auto OL_W_ID = NamedColumn(std::string("OL_W_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(OL_W_ID_CID));
const auto OL_D_ID = NamedColumn(std::string("OL_D_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(OL_D_ID_CID));
const auto OL_O_ID = NamedColumn(std::string("OL_O_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(OL_O_ID_CID));
const auto OL_I_ID = NamedColumn(std::string("OL_I_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(OL_I_ID_CID));
const auto OL_SUPPLY_W_ID = NamedColumn(std::string("OL_SUPPLY_W_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(OL_SUPPLY_W_ID_CID));
const auto OL_QUANTITY = NamedColumn(std::string("OL_QUANTITY"), std::make_shared<UnencodedTableColumn<Integer>>(OL_QUANTITY_CID));
const auto OL_AMOUNT = NamedColumn(std::string("OL_AMOUNT"), std::make_shared<UnencodedTableColumn<Decimal<2>>>(OL_AMOUNT_CID));
const auto OL_DELIVERY_D = NamedColumn(std::string("OL_DELIVERY_D"), std::make_shared<UnencodedTableColumn<DateTime>>(OL_DELIVERY_D_CID));

const auto N_NATIONKEY = NamedColumn(std::string("N_NATIONKEY"), std::make_shared<UnencodedTableColumn<Identifier>>(N_NATIONKEY_CID));
const auto N_NAME = NamedColumn(std::string("N_NAME"), std::make_shared<UnencodedTableColumn<Char<25>>>(N_NAME_CID));

const auto SU_NATIONKEY = NamedColumn(std::string("SU_NATIONKEY"), std::make_shared<UnencodedTableColumn<Identifier>>(SU_NATIONKEY_CID));
const auto SU_SUPPKEY = NamedColumn(std::string("SU_SUPPKEY"), std::make_shared<UnencodedTableColumn<Identifier>>(SU_SUPPKEY_CID));

const auto S_W_ID = NamedColumn(std::string("S_W_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(S_W_ID_CID));
const auto S_I_ID = NamedColumn(std::string("S_I_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(S_I_ID_CID));

const auto O_ID = NamedColumn(std::string("O_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(O_ID_CID));
const auto O_W_ID = NamedColumn(std::string("O_W_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(O_W_ID_CID));
const auto O_D_ID = NamedColumn(std::string("O_D_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(O_D_ID_CID));
const auto O_C_ID = NamedColumn(std::string("O_C_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(O_C_ID_CID));
const auto O_ENTRY_D = NamedColumn(std::string("O_ENTRY_D"), std::make_shared<UnencodedTableColumn<DateTime>>(O_ENTRY_D_CID));
const auto O_CARRIER_ID = NamedColumn(std::string("O_CARRIER_ID"), std::make_shared<UnencodedTableColumn<Identifier>>(O_CARRIER_ID_CID));
const auto O_OL_CNT = NamedColumn(std::string("O_OL_CNT"), std::make_shared<UnencodedTableColumn<Integer>>(O_OL_CNT_CID));


void createTables(DB& db, const ExecutionContext context);
void createIndexes(DB& db, const ExecutionContext context);
void importFromCSV(DB& db, const std::string& path, const ExecutionContext context);
bool validateDatabase(DB& db, bool full);

} // namespace tpcch