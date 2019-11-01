from red_val.parsing import split_into_parts, ParsingError


VARIABLE_SEPARATOR_START = '{{'
VARIABLE_SEPARATOR_END = '}}'

PRIVATE_KEYS = {'access', 'auth'}


class VariableKey:
    def __init__(self, key, protected):
        """
        Creates a new VariableKey

        :param key: The key string of this VariableKey
        :param protected: Indicates if this VariableKey is protected or not
        """
        self.key = key
        self.protected = protected

    def __str__(self):
        return self.key


def get_variable_keys(data):
    """
    Returns a list of variable keys found in the given data.
    A variable key string is a string that starts with '{{' and ends with '}}'.
    To collect the variable keys iterates recursively over data values and appends variable keys to the variable keys
    set.

    :param data: The data to analyse.
    :type data: dict
    :return: A list of unique template keys
    """
    variable_keys = set()
    get_variable_keys_impl(data, variable_keys)
    return unique_variable_keys(variable_keys)


def get_variable_keys_impl(data, variable_keys, key_string=None, variable_keys_allowed=False, protected=False):
    """
    Iterates recursively over data values and appends variable keys to the variable keys list.
    A variable key string is a string that starts with '{{' and ends with '}}'.

    :param data: The data to analyse.
    :param variable_keys: A set of variable to append variable keys to.
    :type variable_keys: set
    :param key_string: A string representing the keys above the current data element.
    :param variable_keys_allowed: A boolean that specifies whether variable keys are allowed in the current dict
                                  position. If a variable key is found but it is not allowed an exception is thrown.
    :param protected: Indicates that the sub keys should be treated as protected keys

    :raise RedVariableError: If a variable key is found, but is not allowed.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            sub_key_string = get_dict_sub_key_string(key, key_string)

            sub_variable_keys_allowed = variable_keys_allowed or (key in PRIVATE_KEYS)
            sub_protected = protected or is_protected_key(key)

            if sub_protected and not sub_variable_keys_allowed:
                raise RedVariableError(
                    'Found protected key "{}", but protected keys are only allowed under one of {}'
                    .format(sub_key_string, str(PRIVATE_KEYS))
                )
            get_variable_keys_impl(
                data=value,
                variable_keys=variable_keys,
                key_string=sub_key_string,
                variable_keys_allowed=sub_variable_keys_allowed,
                protected=sub_protected
            )
    elif isinstance(data, list):
        for index, sub_data in enumerate(data):
            sub_key_string = get_list_sub_key_string(index, key_string)

            get_variable_keys_impl(
                data=sub_data,
                variable_keys=variable_keys,
                key_string=sub_key_string,
                variable_keys_allowed=variable_keys_allowed,
                protected=protected
            )
    elif isinstance(data, str):
        if variable_keys_allowed:
            new_variable_keys = _extract_variable_keys(data, key_string, protected)
            if new_variable_keys:
                variable_keys.update(new_variable_keys)
        elif (VARIABLE_SEPARATOR_START in data) or (VARIABLE_SEPARATOR_END in data):
            raise RedVariableError(
                'Found invalid bracket in "{}" under "{}" in red data. Variable keys are only allowed as sub element of'
                ' an auth or access key.'.format(data, key_string)
            )


def unique_variable_keys(variable_keys):
    """
    Returns a sorted list of variable keys, where every key is unique.

    :param variable_keys: The list of variable keys from which duplicates are to be filtered out
    :type variable_keys: set[VariableKey]
    :return: A list of unique variable keys
    :rtype: list[VariableKey]
    """
    d = {}
    for variable_key in variable_keys:
        if variable_key.protected:
            d[variable_key.key] = variable_key
        else:
            if variable_key.key not in d:
                d[variable_key.key] = variable_key

    variable_key_list = list(d.values())
    variable_key_list.sort(key=lambda key: key.key, reverse=True)
    return variable_key_list


def get_dict_sub_key_string(key, key_string):
    """
    Helper function to add the given key to preceding keys.
    Handles the case, if key is the first key and key_string is None.

    :param key: The key to add to the preceding keys
    :param key_string: The string containing the preceding keys
    :return: A string concatenation of key_string and key
    """
    if key_string is None:
        sub_key_string = key
    else:
        sub_key_string = '{}.{}'.format(key_string, key)
    return sub_key_string


def get_list_sub_key_string(index, key_string):
    """
    Helper function to add the given index to preceding keys.
    Handles the case, if key is the first key and key_string is None.

    :param index: The index to add to the key_string
    :param key_string: The string containing the preceding keys
    :return: A string concatenation of key_string and index
    """
    if key_string is None:
        sub_key_string = '[{}]'.format(index)
    else:
        sub_key_string = '{}[{}]'.format(key_string, index)
    return sub_key_string


def is_protected_key(key):
    """
    Returns whether the given key is a protected key. ('password' or starts with underscore).

    :param key: The key to check
    :return: True, if the given key is a protected key, otherwise False
    """
    return (key == 'password') or (key.startswith('_'))


def _extract_variable_keys(variable_string, key_string, protected):
    """
    Returns a set of variable keys, found inside variable_string

    :param variable_string: The string to analyse
    :type variable_string: str
    :param key_string: The keys of the given variable_string
    :param protected: Indicates whether the extracted keys are protected
    :return: A set of variable keys found inside variable_string

    :raise Parsing: If the variable_string is malformed
    """
    try:
        parts = split_into_parts(variable_string, VARIABLE_SEPARATOR_START, VARIABLE_SEPARATOR_END)
    except ParsingError as e:
        raise RedVariableError(
            'Could not parse variables from string "{}" in "{}". Do not use "{{" or "}}" in strings except for '
            'variable values. Failed with the following message:\n{}'.format(variable_string, key_string, str(e))
        )

    variable_keys = set()

    for part in parts:
        if is_variable_key(part):
            variable_key_string = part[2:-2]
            if (VARIABLE_SEPARATOR_START in variable_key_string) or (VARIABLE_SEPARATOR_END in variable_key_string):
                raise RedVariableError(
                    'Could not parse variable string "{}" in "{}". Too many brackets.'
                    .format(variable_string, key_string)
                )
            if variable_key_string == '':
                raise RedVariableError(
                    'Could not parse variable string "{}" in "{}". Variable keys should not be empty.'
                    .format(variable_string, key_string)
                )
            variable_keys.add(VariableKey(variable_key_string, protected))
        elif (VARIABLE_SEPARATOR_START in part) or (VARIABLE_SEPARATOR_END in part):
            raise RedVariableError(
                'Could not parse variable string "{}" in "{}". Too many brackets.'.format(variable_string, key_string)
            )

    return variable_keys


def is_variable_key(s):
    """
    Returns True if s is a variable string.

    :param s: The string to analyse
    :return: True, if s is a starts with VARIABLE_SEPARATOR_START and ends with VARIABLE_SEPARATOR_END
    """
    return s.startswith(VARIABLE_SEPARATOR_START) and s.endswith(VARIABLE_SEPARATOR_END)


def complete_variables(data, variables, key_string=None):
    """
    Fills the given variables into the given data.

    :param data: The data to complete
    :param variables: The variables to use
    :param key_string: A string representing the keys above the current data element.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            sub_key_string = get_dict_sub_key_string(key, key_string)

            if isinstance(value, str):
                completed_variable_string = _resolve_variable_string(value, variables, sub_key_string)
                data[key] = completed_variable_string
            else:
                complete_variables(value, variables, sub_key_string)
    elif isinstance(data, list):
        for index, value in enumerate(data):
            sub_key_string = get_list_sub_key_string(index, key_string)

            if isinstance(value, str):
                completed_variable_string = _resolve_variable_string(value, variables, sub_key_string)
                data[index] = completed_variable_string
            else:
                complete_variables(value, variables, sub_key_string)


def _resolve_variable_string(variable_string, variables, key_string):
    """
    Replaces the variable keys inside the given variable string.

    :param variable_string: The string in which variables are to be replaced.
    :param variables: The variables to use
    :param key_string: The key string describing where the variable string is found in the red file.
    :return: The variable string with variable keys resolved
    """
    try:
        parts = split_into_parts(variable_string, VARIABLE_SEPARATOR_START, VARIABLE_SEPARATOR_END)
    except ParsingError as e:
        raise RedVariableError(
            'Could not parse variable "{}" in "{}". Failed with the following message:\n{}'
            .format(variable_string, key_string, str(e))
        )
    result = []
    for p in parts:
        if is_variable_key(p):
            resolved = variables[p[2:-2]]
            result.append(resolved)
        else:
            result.append(p)

    return ''.join(result)


class RedVariableError(Exception):
    pass
