"""
Exceptions used in the Hummingbot codebase.
"""


class HummingbotBaseException(Exception):
    """
    Most errors raised in Hummingbot should inherit this class so we can
    differentiate them from errors that come from dependencies.
    """


class ArgumentParserError(HummingbotBaseException):
    """
    Unable to parse a command (like start, stop, etc) from the hummingbot client
    """


class OracleRateUnavailable(HummingbotBaseException):
    """
    Asset value from third party is unavailable
    """


class InvalidScriptModule(HummingbotBaseException):
    """
    The file does not contain a ScriptBase subclass
    """


class InvalidController(HummingbotBaseException):
    """
    The file does not contain a ControllerBase subclass
    """


# === Order Edit Exceptions ===

class OrderEditError(HummingbotBaseException):
    """Base exception for order edit operations"""
    pass


class OrderNotEditableError(OrderEditError):
    """Raised when order state doesn't allow editing (e.g., already filled/cancelled)"""
    pass


class OrderEditCancelFailed(OrderEditError):
    """Raised when cancellation step fails during cancel-replace - original order may still be active"""
    pass


class OrderEditBalanceTimeout(OrderEditError):
    """
    Raised when balance doesn't reflect in time after cancellation.
    CRITICAL: Order is cancelled but replacement cannot proceed.
    """
    pass


class OrderEditReplacementFailed(OrderEditError):
    """
    Raised when replacement order fails after successful cancellation.
    CRITICAL: Order is cancelled with no replacement.
    """
    pass
