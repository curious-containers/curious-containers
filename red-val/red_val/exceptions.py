from jsonschema import ValidationError


def format_validation_error(ex, message=None):
    """
    Returns a string describing the given jsonschema.ValidationError

    :param ex: A jsonschema.ValidationError
    :type ex: ValidationError
    :param message: A message describing the error
    :type message: str
    :rtype: str
    """
    if message is None:
        message = ''

    where = '/'.join([str(s) for s in ex.absolute_path]) if ex.absolute_path else '/'
    return '{}\n\tkey in red file: {}\n\treason: {}'.format(message, where, ex.message)


class RedSpecificationError(Exception):
    pass


class RedValidationError(Exception):
    pass


class CWLSpecificationError(Exception):
    pass
