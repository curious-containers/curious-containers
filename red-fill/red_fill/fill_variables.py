"""
This module provides an implementation to use a keyring service to fill variable keys or to interactively ask the user
to fill in variables.
"""
import sys
from getpass import getpass

import keyring
import keyring.backends.chainer
from keyring.errors import KeyringLocked


def _ask_for_variable_value(variable_key):
    """
    Asks the user interactively for the given key

    :param variable_key: The key to ask for
    :return: The users input
    """
    if variable_key.protected:
        value = getpass('{} (protected): '.format(variable_key.key))
    else:
        value = input('{}: '.format(variable_key.key))
    return value


def get_variables_by_keyring_or_user(variable_keys, keyring_service, fail_if_interactive):
    """
    Returns a dictionary containing variable keys and values.
    To fill in the variable keys, first the keyring service is requested for each key,
    afterwards the user is asked interactively.

    :param variable_keys: A set of variable keys to query the keyring or ask the user
    :type variable_keys: list[VariableKey]
    :param keyring_service: The keyring service to query
    :param fail_if_interactive: Dont ask the user interactively for key values, but fail with an exception
    :return: A dictionary containing a mapping of variable keys and values
    :rtype: dict
    :raise RedVariableError: If not all VariableKeys could be resolved and fail_if_interactive is set
    """
    keyring_usable = _keyring_usable()

    variables = {}
    keys_that_could_not_be_fulfilled = []
    new_interactive_variables = []

    interactive_keys_present = False

    for variable_key in variable_keys:
        # try keyring
        variable_value = None
        if keyring_usable:
            try:
                variable_value = keyring.get_password(keyring_service, variable_key.key)
            except KeyringLocked:
                keyring_usable = False

        if variable_value is not None:
            variables[variable_key.key] = variable_value
        else:  # ask user
            if fail_if_interactive:
                keys_that_could_not_be_fulfilled.append(variable_key.key)
                continue

            if not interactive_keys_present:
                print('Asking for variables:')
                sys.stdout.flush()
                interactive_keys_present = True

            variable_value = _ask_for_variable_value(variable_key)
            variables[variable_key.key] = variable_value

            new_interactive_variables.append((variable_key.key, variable_value))

    if keys_that_could_not_be_fulfilled:
        raise UnfilledVariablesError(
            'Could not resolve the following variables: "{}".'.format(keys_that_could_not_be_fulfilled)
        )

    if interactive_keys_present and keyring_usable:
        answer = input('Add variables to keyring "{}" [y/N]: '.format(keyring_service))
        if (answer.lower() == 'y') or (answer.lower() == 'yes'):
            for new_key, new_value in new_interactive_variables:
                keyring.set_password(keyring_service, new_key, new_value)
            print('Added variables to keyring.')

    return variables


def _keyring_usable():
    """
    :return: whether keyring is usable or not. Checks whether the chainer backend has backends configured
    :rtype: bool
    """
    return bool(keyring.backends.chainer.ChainerBackend.backends)


class UnfilledVariablesError(Exception):
    pass
