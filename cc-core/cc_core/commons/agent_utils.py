"""
This module defines functions for the following functionalities:
- Filter batch inputs for inputs, which define a connector (in contrast to inputs defining primitives)
"""


BLUE_INPUT_CLASSES = {'File', 'Directory'}


def _create_blue_batch_inputs(batch_inputs):
    """
    Filters the given inputs and returns only batch inputs, that contain a connector definition

    :param batch_inputs: The batch inputs of a red file
    :return: A dictionary containing all keys of batch_inputs, which contain a connector definition
    """
    blue_batch_inputs = {}
    for input_key, input_value in batch_inputs.items():
        if _is_blue_input_value(input_value):
            blue_batch_inputs[input_key] = input_value
    return blue_batch_inputs


def _is_blue_input_value(input_value):
    """
    Returns whether the given input value defines a connector.

    :param input_value: The input value as list or value, that may contain a connector
    :return: True, if input value contains a connector definition, otherwise false
    """
    if isinstance(input_value, list):
        if not input_value:
            return False

        for sub_input_value in input_value:
            if not _is_blue_input_value(sub_input_value):
                return False
        return True
    elif isinstance(input_value, dict):
        return input_value.get('class') in BLUE_INPUT_CLASSES

    return False
