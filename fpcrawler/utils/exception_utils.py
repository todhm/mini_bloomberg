class ReportLinkParseError(Exception):
    pass


class NotableError(Exception):
    pass


class ExpectedError(Exception):
    pass

class UnexpectedError(Exception):
    pass


class NotSufficientError(Exception):
    pass

class NotSufficientCashflowError(NotSufficientError):
    pass
class NotSufficientBalanceSheetError(NotSufficientError):
    pass
class NotSufficientIncomeStatementError(NotSufficientError):
    pass
class ReturnSoupError(Exception):
    pass

class TableStyleError(Exception):
    pass


class RequestError(Exception):
    pass

class NoDataError(Exception):
    pass


class StockDataError(Exception):
    pass
