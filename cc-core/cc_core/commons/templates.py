from cc_core.commons.parsing import split_into_parts
from cc_core.commons.exceptions import TemplateError, ParsingError
from cc_core.commons.red import check_keys_are_strings

TEMPLATE_SEPARATOR_START = '{{'
TEMPLATE_SEPARATOR_END = '}}'

PRIVATE_KEYS = {'access', 'auth'}


class TemplateKey:
    def __init__(self, key, protected):
        """
        Creates a new TemplateKey
        :param key: The key string of this TemplateKey
        :param protected: Indicates if this TemplateKey is protected or not
        """
        self.key = key
        self.protected = protected

    def __str__(self):
        return self.key


def get_dict_sub_key_string(key, key_string):
    if key_string is None:
        sub_key_string = key
    else:
        sub_key_string = '{}.{}'.format(key_string, key)
    return sub_key_string


def get_list_sub_key_string(index, key_string):
    if key_string is None:
        sub_key_string = '[{}]'.format(index)
    else:
        sub_key_string = '{}[{}]'.format(key_string, index)
    return sub_key_string


def get_secret_values(red_data):
    """
    Returns a list of secret values found in the given red data.
    A secret value is a value found under a protected key
    :param red_data: A dictionary containing the red data
    :return: A list of secret values found in the given red data
    """
    check_keys_are_strings(red_data)
    secret_values = []
    _append_secret_values(red_data, secret_values)
    return secret_values


def _append_secret_values(data, secret_values, protected=False):
    """
    Appends secret values found in data to secret_values
    :param data: The data to search in for secret values
    :param secret_values: The list of secret values
    :param protected: Indicates if the given value is protected or not
    """
    if isinstance(data, dict):
        for key, value in data.items():
            sub_protected = protected or is_protected_key(key)
            _append_secret_values(value, secret_values, sub_protected)
    elif isinstance(data, list):
        for value in data:
            _append_secret_values(value, secret_values, protected)
    else:
        if protected:
            secret_values.append(data)


def get_template_keys(data, template_keys, key_string=None, template_keys_allowed=False, protected=False):
    """
    Iterates recursively over data values and appends template keys to the template keys list.
    A template key string is a string that starts with '{{' and ends with '}}'.

    :param data: The data to analyse.
    :param template_keys: A set of template keys to append template keys to.
    :type template_keys: set
    :param key_string: A string representing the keys above the current data element.
    :param template_keys_allowed: A boolean that specifies whether template keys are allowed in the current dict
    position. If a template key is found but it is not allowed an exception is thrown.
    :param protected: Indicates that the sub keys should be treated as protected keys
    :raise TemplateError: If a template key is found, but is not allowed.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            sub_key_string = get_dict_sub_key_string(key, key_string)

            sub_template_keys_allowed = template_keys_allowed or (key in PRIVATE_KEYS)
            sub_protected = protected or is_protected_key(key)

            if sub_protected and not sub_template_keys_allowed:
                raise TemplateError('Found protected key "{}", but protected keys are only allowed under one of {}'
                                    .format(sub_key_string, str(PRIVATE_KEYS)))
            get_template_keys(
                data=value,
                template_keys=template_keys,
                key_string=sub_key_string,
                template_keys_allowed=sub_template_keys_allowed,
                protected=sub_protected
            )
    elif isinstance(data, list):
        for index, sub_data in enumerate(data):
            sub_key_string = get_list_sub_key_string(index, key_string)

            get_template_keys(
                data=sub_data,
                template_keys=template_keys,
                key_string=sub_key_string,
                template_keys_allowed=template_keys_allowed,
                protected=protected
            )
    elif isinstance(data, str):
        if template_keys_allowed:
            new_template_keys = _extract_template_keys(data, key_string, protected)
            if new_template_keys:
                template_keys.update(new_template_keys)
        elif (TEMPLATE_SEPARATOR_START in data) or (TEMPLATE_SEPARATOR_END in data):
            raise TemplateError('Found invalid bracket in "{}" under "{}" in red data. Template keys are only '
                                'allowed as sub element of an auth or access key.'.format(data, key_string))


def is_protected_key(key):
    """
    Returns whether the given key is a protected key. ('password' or starts with underscore).
    :param key: The key to check
    :return: True, if the given key is a protected key, otherwise False
    """
    return (key == 'password') or (key.startswith('_'))


def is_template_key(s):
    """
    Returns True if s is a template string.
    :param s: The string to analyse
    :return: True, if s is a starts with TEMPLATE_SEPARATOR_START and ends with TEMPLATE_SEPARATOR_END
    """
    return s.startswith(TEMPLATE_SEPARATOR_START) and s.endswith(TEMPLATE_SEPARATOR_END)


def _extract_template_keys(template_string, key_string, protected):
    """
    Returns a set of template keys, found inside template_string.
    :param template_string: The string to analyse
    :type template_string: str
    :param key_string: The keys of the given template_string
    :param protected: Indicates whether the extracted keys are protected
    :return: A set of template keys found inside template_string
    :raise Parsing: If the template_string is malformed
    """
    try:
        parts = split_into_parts(template_string, TEMPLATE_SEPARATOR_START, TEMPLATE_SEPARATOR_END)
    except ParsingError as e:
        raise TemplateError('Could not parse template string "{}" in "{}". Do not use "{{" or "}}" in strings except '
                            'for template values. Failed with the following message:\n{}'
                            .format(template_string, key_string, str(e)))

    template_keys = set()

    for part in parts:
        if is_template_key(part):
            template_key_string = part[2:-2]
            if (TEMPLATE_SEPARATOR_START in template_key_string) or (TEMPLATE_SEPARATOR_END in template_key_string):
                raise TemplateError('Could not parse template string "{}" in "{}". Too many brackets.'
                                    .format(template_string, key_string))
            if template_key_string == '':
                raise TemplateError('Could not parse template string "{}" in "{}". Template keys should not be empty.'
                                    .format(template_string, key_string))
            template_keys.add(TemplateKey(template_key_string, protected))
        elif (TEMPLATE_SEPARATOR_START in part) or (TEMPLATE_SEPARATOR_END in part):
            raise TemplateError('Could not parse template string "{}" in "{}". Too many brackets.'
                                .format(template_string, key_string))

    return template_keys


def normalize_keys(data):
    """
    Removes starting underscores from the keys in data
    :param data: The data in which keys with underscores should be replaced without underscore
    """
    if isinstance(data, dict):
        keys = list(data.keys())
        for key in keys:
            value = data[key]
            if key.startswith('_'):
                normalized_key = key[1:]
                data[normalized_key] = value
                del data[key]
            normalize_keys(value)
    elif isinstance(data, list):
        for value in data:
            normalize_keys(value)
