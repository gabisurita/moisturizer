class BaseError(Exception):
    table = None


class ModelInferenceException(BaseError):
    def __init__(self, type_id=None):
        self.type_id = type_id


class ModelNotExists(ModelInferenceException):
    def __str__(self):
        return "Table {} does not exists.".format(self.table)


class AuthenticationError(ModelInferenceException):
    def __str__(self):
        return "Unrecognized authentication key.".format(self.table)


def parse_exception(exception):
    return {
        'message': str(exception),
        'type': getattr(exception, 'type_id', None),
        'error_code': exception.__class__.__name__,
    }


def format_http_error(http_error, exception):
    return http_error(json={
        'error': parse_exception(exception)
    })
