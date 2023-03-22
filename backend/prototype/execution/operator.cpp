#include "operator.hpp"

OperatorBase::OperatorBase() { }

OperatorBase::~OperatorBase() { }

void OperatorBase::setNextOperator(std::shared_ptr<OperatorBase> next_operator) {
    if (this->next_operator != nullptr) {
        throw std::runtime_error("Next operator already set");
    }
    this->next_operator = std::move(next_operator);
}

std::shared_ptr<OperatorBase> OperatorBase::getNextOperator() const {
    return next_operator;
}