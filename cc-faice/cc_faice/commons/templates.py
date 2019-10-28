import sys
from getpass import getpass

import keyring
import keyring.backends.chainer
from keyring.errors import KeyringLocked

from cc_core.commons.exceptions import TemplateError, ParsingError
from cc_core.commons.parsing import split_into_parts
from cc_core.commons.templates import TEMPLATE_SEPARATOR_START, TEMPLATE_SEPARATOR_END, get_dict_sub_key_string, \
    get_list_sub_key_string, get_template_keys, is_template_key


def complete_red_templates(red_data, keyring_service, fail_if_interactive):
    """
    Replaces templates inside the given red data. Requests the template keys of the red data from the keyring using the
    given keyring service.

    :param red_data: The red data to complete template keys for
    :type red_data: dict[str, Any]
    :param keyring_service: The keyring service to use for requests
    :type keyring_service: str
    :param fail_if_interactive: Dont ask the user interactively for key values, but fail with an exception
    """
    template_keys = set()
    get_template_keys(red_data, template_keys)
    template_keys = unique_template_keys(template_keys)

    templates = _get_templates(template_keys, keyring_service, fail_if_interactive)
    _complete_templates(red_data, templates)


def unique_template_keys(template_keys):
    d = {}
    for template_key in template_keys:
        if template_key.protected:
            d[template_key.key] = template_key
        else:
            if template_key.key not in d:
                d[template_key.key] = template_key

    template_key_list = [val for _, val in d.items()]
    template_key_list.sort(key=lambda key: key.key, reverse=True)
    return template_key_list


def _ask_for_template_value(template_key):
    """
    Asks the user interactively for the given key
    :param template_key: The key to ask for
    :return: The users input
    """
    if template_key.protected:
        value = getpass('{} (protected): '.format(template_key.key))
    else:
        value = input('{}: '.format(template_key.key))
    return value


def _get_templates(template_keys, keyring_service, fail_if_interactive):
    """
    Returns a dictionary containing template keys and values.
    To fill in the template keys, first the keyring service is requested for each key,
    afterwards the user is asked interactively.

    :param template_keys: A set of template keys to query the keyring or ask the user
    :type template_keys: list[TemplateKey]
    :param keyring_service: The keyring service to query
    :param fail_if_interactive: Dont ask the user interactively for key values, but fail with an exception
    :return: A dictionary containing a mapping of template keys and values
    :rtype: dict
    :raise TemplateError: If not all TemplateKeys could be resolved and fail_if_interactive is set
    """
    keyring_usable = _keyring_usable()

    templates = {}
    keys_that_could_not_be_fulfilled = []
    new_interactive_templates = []

    interactive_keys_present = False

    for template_key in template_keys:
        # try keyring
        template_value = None
        if keyring_usable:
            try:
                template_value = keyring.get_password(keyring_service, template_key.key)
            except KeyringLocked:
                keyring_usable = False

        if template_value is not None:
            templates[template_key.key] = template_value
        else:  # ask user
            if fail_if_interactive:
                keys_that_could_not_be_fulfilled.append(template_key.key)
                continue

            if not interactive_keys_present:
                print('Asking for variables:')
                sys.stdout.flush()
                interactive_keys_present = True

            template_value = _ask_for_template_value(template_key)
            templates[template_key.key] = template_value

            new_interactive_templates.append((template_key.key, template_value))

    if keys_that_could_not_be_fulfilled:
        raise TemplateError('Could not resolve the following variables: "{}".'
                            .format(keys_that_could_not_be_fulfilled))

    if interactive_keys_present and keyring_usable:
        answer = input('Add variables to keyring "{}" [y/N]: '.format(keyring_service))
        if (answer.lower() == 'y') or (answer.lower() == 'yes'):
            for new_key, new_value in new_interactive_templates:
                keyring.set_password(keyring_service, new_key, new_value)
            print('Added variables to keyring.')

    return templates


def _keyring_usable():
    """
    :return: whether keyring is usable or not. Checks whether the chainer backend has backends configured
    :rtype: bool
    """
    return bool(keyring.backends.chainer.ChainerBackend.backends)


def _resolve_template_string(template_string, templates, key_string):
    """
    Replaces the template keys inside the given template string.
    :param template_string: The string in which templates are to be replaced.
    :param templates: The templates to use
    :param key_string: The key string describing where the template string is found in the red file.
    :return: The template string with template keys resolved
    """
    try:
        parts = split_into_parts(template_string, TEMPLATE_SEPARATOR_START, TEMPLATE_SEPARATOR_END)
    except ParsingError as e:
        raise TemplateError('Could not parse template "{}" in "{}". Failed with the following message:\n{}'
                            .format(template_string, key_string, str(e)))
    result = []
    for p in parts:
        if is_template_key(p):
            resolved = templates[p[2:-2]]
            result.append(resolved)
        else:
            result.append(p)

    return ''.join(result)


def _complete_templates(data, templates, key_string=None):
    """
    Fills the given templates into the given data.
    :param data: The data to complete
    :param templates: The templates to use
    :param key_string: A string representing the keys above the current data element.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            sub_key_string = get_dict_sub_key_string(key, key_string)

            if isinstance(value, str):
                completed_template_string = _resolve_template_string(value, templates, sub_key_string)
                data[key] = completed_template_string
            else:
                _complete_templates(value, templates, sub_key_string)
    elif isinstance(data, list):
        for index, value in enumerate(data):
            sub_key_string = get_list_sub_key_string(index, key_string)

            if isinstance(value, str):
                completed_template_string = _resolve_template_string(value, templates, sub_key_string)
                data[index] = completed_template_string
            else:
                _complete_templates(value, templates, sub_key_string)
