from red_val.red_validation import check_keys_are_strings
from red_val.red_variables import is_protected_key


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
