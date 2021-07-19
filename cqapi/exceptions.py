class QueryTranslationError(Exception):
    pass

class SavedQueryTranslationError(QueryTranslationError):
    pass

class CqApiError(BaseException):
    pass


class ConqueryClientConnectionError(CqApiError):
    def __init__(self, msg):
        self.message = msg

class QueryNotFoundError(BaseException):
    pass