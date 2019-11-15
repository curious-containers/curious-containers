from red_fill.fill_variables import get_variables_by_keyring_or_user
from red_val.red_variables import get_variable_keys, complete_variables


def complete_red_variables(red_data, keyring_service, fail_if_interactive):
    """
    Replaces variables inside the given red data. Requests the variable keys of the red data from the keyring using the
    given keyring service.

    :param red_data: The red data to complete variable keys for
    :type red_data: dict[str, Any]
    :param keyring_service: The keyring service to use for requests
    :type keyring_service: str
    :param fail_if_interactive: Dont ask the user interactively for key values, but fail with an exception
    """
    variable_keys = get_variable_keys(red_data)
    variables = get_variables_by_keyring_or_user(variable_keys, keyring_service, fail_if_interactive)
    complete_variables(red_data, variables)
