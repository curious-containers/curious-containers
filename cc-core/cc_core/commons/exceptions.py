import sys

import re
import traceback
from traceback import format_exc

from jsonschema import ValidationError
from red_val.exceptions import format_validation_error


def _hide_secret_values(text, secret_values):
    """
    Replaces occurrences of secret values in the given text with asterisks. Empty secret
    values or values consisting of only whitespace are not replaced.

    :param text: The original text containing potential secret values.
    :type text: str
    :param secret_values: A list of secret values to be replaced.
    :type secret_values: list
    :return: The error output where secret_values are replaced with asterisks
    :rtype: str
    """
    secret_values = list(filter(lambda string: string.strip() != "", secret_values))
    if secret_values:
        return re.sub('|'.join(secret_values), '********', text)
    return text


def _lstrip_quarter(s):
    len_s = len(s)
    s = s.lstrip()
    len_s_strip = len(s)
    quarter = (len_s - len_s_strip) // 4
    return ' ' * quarter + s


def exception_format(secret_values=None):
    exc_text = format_exc()
    exc_text = _hide_secret_values(exc_text, secret_values)
    return [_lstrip_quarter(elem.replace('"', '').replace("'", '').rstrip()) for elem in exc_text.split('\n') if elem]


def full_class_name(o):
    """
    Returns the full qualified name of the class of o.

    :param o: The object whose class name should be returned
    :return: A str representing the class of o
    :rtype: str
    """
    module = o.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return o.__class__.__name__  # Avoid reporting __builtin__
    else:
        return module + '.' + o.__class__.__name__


def log_format_exception(e):
    """
    Returns a formatted string describing the given error for logging purposes.

    :param e: The exception to format
    :return: A string containing a stacktrace and brief exception text
    :rtype: str
    """
    tb_list = traceback.extract_tb(e.__traceback__)

    max_filename_len = 0
    for i in tb_list:
        v = len(i.filename) + len(str(i.lineno))
        if max_filename_len < v:
            max_filename_len = v

    text_l = []

    for tb in tb_list:
        text_l.append(('  In {:' + str(max_filename_len+2) + '} in {}()').format(
            '{}:{}'.format(tb.filename, tb.lineno),
            tb.name
        ))

    if isinstance(e, ValidationError):
        message_str = format_validation_error(e, 'Schema Validation failed.')
    else:
        message_str = str(e)

    text_l.append('[{}]: {}'.format(full_class_name(e), message_str))
    return '\n'.join(text_l)


def brief_exception_text(exception, secret_values=None):
    """
    Returns the Exception class and the message of the exception as string.

    :param exception: The exception to format
    :param secret_values: Values to hide in output
    """
    exception_text = _hide_secret_values(str(exception), secret_values)
    return '[{}]\n{}'.format(type(exception).__name__, exception_text)


def print_exception(exception, secret_values=None):
    """
    Prints the exception message and the name of the exception class to stderr.

    :param exception: The exception to print
    :param secret_values: Values to hide in output
    """
    print(brief_exception_text(exception, secret_values), file=sys.stderr)


class InvalidInputReference(Exception):
    pass


class ArgumentError(Exception):
    pass


class AgentError(Exception):
    pass


class EngineError(Exception):
    pass


class FileError(Exception):
    pass


class DirectoryError(Exception):
    pass


class JobExecutionError(Exception):
    pass


class JobSpecificationError(Exception):
    pass


class ConnectorError(Exception):
    pass


class AccessValidationError(Exception):
    pass


class AccessError(Exception):
    pass


class InvalidExecutionEngineArgumentException(Exception):
    pass
