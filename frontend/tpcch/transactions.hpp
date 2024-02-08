#pragma once

#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/scheduling/execution_context.hpp"

struct OrderLine {
    Identifier ol_i_id;
    Identifier ol_supply_w_id;
    bool ol_is_remote;
    Integer ol_quantity;
};

class DefaultBreaker;
class ExecutablePipeline;

namespace tpcch {

// helper method for synchronous QEP execution in the calling thread
std::shared_ptr<DefaultBreaker> executeSynchronouslyWithDefaultBreaker(DB& db, std::vector<std::unique_ptr<ExecutablePipeline>>&& pipelines, const ExecutionContext context);
// transactions
bool runNewOrder(std::ostream& log, DB& db, Identifier w_id, Identifier d_id, Identifier c_id, const OrderLine* orderlines, uint32_t ol_cnt, bool all_local, DateTime o_entry_d, const ExecutionContext context);
bool runPayment(std::ostream& log, DB& db, Identifier w_id, Identifier d_id, Identifier c_w_id, Identifier c_d_id, bool customer_based_on_last_name, Identifier c_id, const std::string& c_last, Decimal<2> h_amount, DateTime h_date, const ExecutionContext context);
bool runOrderStatus(std::ostream& log, DB& db, Identifier w_id, Identifier d_id, bool customer_based_on_last_name, Identifier c_id, const std::string& c_last_in, const ExecutionContext context);
bool runDelivery(std::ostream& log, DB& db, Identifier w_id, Identifier carrier_id, DateTime ol_delivery_d, const ExecutionContext context);
bool runStockLevel(std::ostream& log, DB& db, Identifier w_id, Identifier d_id, int32_t threshold, const ExecutionContext context);

} // namespace tpcch