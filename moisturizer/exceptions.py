class BaseError(Exception):
    table = None


class ModelInferenceException(BaseError):
    def __init__(self, table=None):
        self.table = table


class ModelNotExists(ModelInferenceException):
    def __str__(self):
        return "Table {} does not exists.".format(self.table)


class AuthenticationError(ModelInferenceException):
    def __str__(self):
        return "Unrecognized authentication key.".format(self.table)


def parse_exception(exception):
    return {
        'message': str(exception),
        'table': getattr(exception, 'table', None),
        'type': exception.__class__.__name__,
    }


def format_http_error(http_error, exception):
    return http_error(json={
        'error': parse_exception(exception)
    })
