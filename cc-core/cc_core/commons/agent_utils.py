"""
This module defines functions for the following functionalities:
- Filter batch inputs for inputs, which define a connector (in contrast to inputs defining primitives)
"""




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
