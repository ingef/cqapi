class QueryTranslationError(Exception):
    pass


class SavedQueryTranslationError(QueryTranslationError):
    pass


class ExternalQueryTranslationError(QueryTranslationError):
    pass


class CqApiError(BaseException):
    pass


class ConqueryClientConnectionError(CqApiError):
    def __init__(self, msg):
        self.message = msg


class QueryNotFoundError(BaseException):
    pass


class RetryCountExceeded(BaseException):
    def __init__(self, msg):
        self.message = msg
    pass


class ConqueryTimeout(BaseException):
    """
        Defines a Timeout Exception for Conquery.
        This is use to return a less verbose Error message compared to requests.exceptions.timeout
    """
    def __init__(self, msg):
        self.message = msg

    pass
