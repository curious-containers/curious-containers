import sys

import re
import traceback
from traceback import format_exc


def _hide_secret_values(text, secret_values):
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
    return [_lstrip_quarter(l.replace('"', '').replace("'", '').rstrip()) for l in exc_text.split('\n') if l]


def log_format_exception(e):
    tb_list = traceback.extract_tb(e.__traceback__)
    last = tb_list[len(tb_list) - 1]
    return 'In "{}:{}" in {}():\n[{}]: {}'.format(last.filename, last.lineno, last.name, type(e).__name__, str(e))


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
