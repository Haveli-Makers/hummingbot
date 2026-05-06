from hummingbot.strategy.strategy_base cimport StrategyBase

cdef class StrategyPyBase(StrategyBase):
    cdef c_did_edit_order(self, object order_edited_event)
    cdef c_did_fail_order_edit(self, object order_edit_failed_event)
