import itertools
import os

import jsonschema
from jsonschema.exceptions import ValidationError

from cc_core.commons.red_to_blue import InputType, OutputType
from cc_core.version import RED_VERSION
from cc_core.commons.schemas.red import red_schema
from cc_core.commons.exceptions import ArgumentError, RedValidationError, CWLSpecificationError
from cc_core.commons.exceptions import RedSpecificationError

SEND_RECEIVE_SPEC_ARGS = ['access', 'internal']
SEND_RECEIVE_SPEC_KWARGS = []
SEND_RECEIVE_VALIDATE_SPEC_ARGS = ['access']
SEND_RECEIVE_VALIDATE_SPEC_KWARGS = []

SEND_RECEIVE_DIRECTORY_SPEC_ARGS = ['access', 'internal', 'listing']
SEND_RECEIVE_DIRECTORY_SPEC_KWARGS = []
SEND_RECEIVE_DIRECTORY_VALIDATE_SPEC_ARGS = ['access']
SEND_RECEIVE_DIRECTORY_VALIDATE_SPEC_KWARGS = []


# def _red_listing_validation(listing):
#     """
#     Raises an RedValidationError, if the given listing does not comply with cwl_job_listing_schema.
#     If listing is None or an empty list, no exception is thrown.
#
#     :param listing: The listing to validate
#     :raise RedValidationError: If the given listing does not comply with cwl_job_listing_schema
#     """
#
#     if listing:
#         try:
#             jsonschema.validate(listing, cwl_job_listing_schema)
#         except ValidationError as e:
#             where = '.'.join([str(s) for s in e.absolute_path]) if e.absolute_path else '/'
#             raise RedValidationError(
#                 'listing does not comply with jsonschema:\n\tkey: {}\n\treason: {}'
#                 .format(where, e.message)
#             )


def red_get_mount_connectors_from_inputs(inputs):
    keys = []
    for input_key, arg in inputs.items():
        arg_items = []

        if isinstance(arg, dict):
            arg_items.append(arg)

        elif isinstance(arg, list):
            arg_items += [i for i in arg if isinstance(i, dict)]

        for i in arg_items:
            connector_data = i['connector']
            if connector_data.get('mount'):
                keys.append(input_key)

    return keys


def _check_red_version(red_version):
    if not red_version == RED_VERSION:
        raise RedSpecificationError(
            'red version "{}" specified in REDFILE is not compatible with red version "{}" of cc-faice'.format(
                red_version, RED_VERSION
            )
        )


class CliJobPair:
    """
    A CliJobPair represents a cli description of one input/output key and a corresponding input/output batch value.
    """
    def __init__(self, key, is_input, cli_description, job_value):
        """
        Initializes a CliJobPair
        :param key: The input/output key of the cli description and job value
        :param is_input: Defines whether it is an input or an output key
        :param cli_description: A dictionary containing a cli description of an input/output key.
        Keys of this cli description are ['type', 'inputBinding']
        :param job_value: A primitive type or a dictionary containing a job value of an input or output
        file/directory. In case of a file/directory the keys of the dictionary are ['class', 'connector']
        """
        self.key = key
        self.is_input = is_input
        self.cli_description = cli_description
        self.job_value = job_value

    def _get_input_output(self):
        return 'input' if self.is_input else 'output'

    def check_type(self):
        """
        Checks whether the job value type fits to the cli description
        :raises RedSpecificationError: if cli_description and job value have incompatible types
        """
        try:
            if self.is_input:
                _check_input_type(self.job_value, self.cli_description['type'])
            else:
                _check_output_type(self.job_value, self.cli_description['type'])
        except RedSpecificationError as e:
            raise RedSpecificationError(
                'Error while checking {} key "{}":\n{}'.format(self._get_input_output(), self.key, str(e))
            )

    def check_directory_listing(self):
        """
        Validates a possible directory listing

        :raise RedValidationError: If listing does not match given job data
        """
        if self.is_input:
            cli_type = InputType.from_string(self.cli_description['type'])
        else:
            cli_type = OutputType.from_string(self.cli_description['type'])

        # handle job value always as array
        if cli_type.is_array():
            job_values = self.job_value
        else:
            job_values = [self.job_value]

        # for job_value in job_values:
        #     if cli_type.is_directory() and job_value is not None:
        #         try:
        #             _red_listing_validation(job_value.get('listing'))
        #         except RedValidationError as e:
        #             raise RedValidationError(
        #                 'Error while checking listing for {} key "{}":\n{}'
        #                 .format(self._get_input_output(), self.key, str(e))
        #             )


def _create_cli_job_pairs(red_data, ignore_outputs):
    """
    Creates one list for all input cli_job pairs and one for all output cli_job pairs given in red data and returns them
    as tuple. If ignore_outputs is True, the output cli_job pair list is empty.

    :param red_data: The red data to get cli job pairs from
    :param ignore_outputs: Whether to ignore outputs or not
    :return: A tuple(input_cli_job_pairs, output_cli_job_pairs) with:
    input_cli_job_pairs: A list of all CliJobPairs given in the input section of red_data
    output_cli_job_pairs: A list of all CliJobPairs given in the output section of red_data
    :rtype: Tuple[List[CliJobPair], List[CliJobPair]]
    :raise RedSpecificationError: If there is a job value, but no corresponding cli description
    """
    batches = red_data.get('batches')
    if batches is None:
        batches = [{
            'inputs': red_data['inputs'],
            'outputs': red_data.get('outputs')
        }]

    input_cli_job_pairs = []

    # input cli job pairs
    cli_inputs = red_data['cli']['inputs']
    for batch in batches:
        job_inputs = batch['inputs']
        input_keys = set.union(set(batch['inputs'].keys()), set(cli_inputs.keys()))

        for input_key in input_keys:
            if input_key not in cli_inputs:
                raise RedSpecificationError(
                    'Input key "{}" is used in job description, but is not given in cli description'.format(input_key)
                )

            input_cli_job_pair = CliJobPair(input_key, True, cli_inputs[input_key], job_inputs.get(input_key))
            input_cli_job_pairs.append(input_cli_job_pair)

    output_cli_job_pairs = []

    # output cli job pairs
    if not ignore_outputs:
        cli_outputs = red_data['cli']['outputs']

        for batch in batches:
            job_outputs = batch.get('outputs')
            if job_outputs is None:
                continue
            output_keys = set.union(set(batch['outputs'].keys()), set(cli_outputs.keys()))

            for output_key in output_keys:
                if output_key not in cli_outputs:
                    raise RedSpecificationError(
                        'Output key "{}" is used in job description, but is not given in cli description'
                        .format(output_key)
                    )

                output_cli_job_pair = CliJobPair(output_key, False, cli_outputs[output_key], job_outputs.get(output_key))
                output_cli_job_pairs.append(output_cli_job_pair)

    return input_cli_job_pairs, output_cli_job_pairs


def red_validation(red_data, ignore_outputs, container_requirement=False):
    """
    Checks the given red data. The process implements the following steps:

    - check if all keys in the red data are strings
    - validate red data with schema
    - match red file version and cc_core version
    - check if all given job values have a corresponding cli description
    - match types of cli description and job values
    - validate listings given in connectors
    - validate container requirement
    - checks if globs do start with "/"

    :param red_data: The red data to check
    :param ignore_outputs: Whether the outputs section should be ignored
    :param container_requirement: If True, this function checks, if there is a container section in the red file
    :raise RedValidationError, RedSpecificationError, CWLSpecificationError: If the red data is not valid
    """
    check_keys_are_strings(red_data)

    try:
        jsonschema.validate(red_data, red_schema)
    except ValidationError as e:
        where = '/'.join([str(s) for s in e.absolute_path]) if e.absolute_path else '/'
        raise RedValidationError(
            'REDFILE does not comply with jsonschema:\n\tkey in red file: {}\n\treason: {}'.format(where, e.message)
        )

    _check_red_version(red_data['redVersion'])

    input_cli_job_pairs, output_cli_job_pairs = _create_cli_job_pairs(red_data, ignore_outputs)

    # check whether types of job data do fit to cli description
    for cli_job_pair in itertools.chain(input_cli_job_pairs, output_cli_job_pairs):
        cli_job_pair.check_type()
        cli_job_pair.check_directory_listing()

    if container_requirement:
        if not red_data.get('container'):
            raise RedSpecificationError('container engine description is missing in REDFILE')

    _check_output_glob(red_data)


def _check_output_glob(red_data):
    """
    Raises an CwlSpecificationError, if a glob is given as absolute path.
    :param red_data: The red data to analyse
    """
    cli_outputs = red_data['cli'].get('outputs')
    if cli_outputs:
        for output_key, output_value in cli_outputs.items():
            if output_value['type'] == 'stdout' or output_value['type'] == 'stderr':
                continue
            glob = output_value['outputBinding']['glob']
            if os.path.isabs(glob):
                raise CWLSpecificationError(
                    'Glob of output key "{}" starts with "/", which is illegal'.format(output_key)
                )


CWL_INPUT_TYPE_TO_PYTHON_TYPE = {
    InputType.InputCategory.File: {dict},
    InputType.InputCategory.Directory: {dict},
    InputType.InputCategory.string: {str},
    InputType.InputCategory.int: {int},
    InputType.InputCategory.long: {int},
    InputType.InputCategory.float: {float, int},
    InputType.InputCategory.double: {float, int},
    InputType.InputCategory.boolean: {bool},
}

CWL_OUTPUT_TYPE_TO_PYTHON_TYPE = {
    OutputType.OutputCategory.File: {dict},
    OutputType.OutputCategory.Directory: {dict},
    OutputType.OutputCategory.stdout: {dict},
    OutputType.OutputCategory.stderr: {dict}
}


def _check_input_type(input_value, cli_description_type):
    """
    Checks whether the type of the given input value matches the type of the given cli description.

    :param input_value: The input value whose type to check
    :param cli_description_type: The cwl type description of the input key
    :raise RedSpecificationError: If actual input type does not match type of cli description
    """
    input_type = InputType.from_string(cli_description_type)

    # check for optional arguments
    if input_value is None:
        if input_type.is_optional():
            return
        raise RedSpecificationError('job value is missing and not optional')

    # check for arrays
    # after this block input_value is always an array and an error is thrown, if input value has wrong list type
    if input_type.is_array():
        if not isinstance(input_value, list):
            raise RedSpecificationError('cli is declared as array, but value is not given as such')
    else:
        if isinstance(input_value, list):
            raise RedSpecificationError('cli is not declared as array, but value is given as array')
        input_value = [input_value]

    for sub_input_value in input_value:
        set_of_possible_value_types = CWL_INPUT_TYPE_TO_PYTHON_TYPE[input_type.input_category]
        if type(sub_input_value) not in set_of_possible_value_types:
            if isinstance(sub_input_value, dict):
                short_repr = 'dictionary'
            elif isinstance(sub_input_value, list):
                short_repr = 'list'
            else:
                short_repr = 'value "{}" of type "{}"'.format(sub_input_value, type(sub_input_value).__name__)

            raise RedSpecificationError('Value should have type "{}", but found "{}".'.format(
                    input_type.input_category.name, short_repr
            ))

        if not input_type.is_primitive():
            cli_type = input_type.input_category.name
            value_type = sub_input_value.get('class')
            if cli_type != value_type:
                raise RedSpecificationError('Is declared as "{}" but given as "{}"'.format(
                    cli_type, value_type
                ))


def _check_output_type(output_value, cli_description_type):
    """
    Checks whether the type of the given output value matches the type of the given cli description.
    :param output_value: The output value whose type to check
    :param cli_description_type: The cwl type description of the output key
    :raise RedSpecificationError: If actual output type does not match type of cli description
    """
    output_type = OutputType.from_string(cli_description_type)

    # check for optional arguments
    if output_value is None:
        if output_type.is_optional():
            return
        raise RedSpecificationError('job value is missing and not optional')

    set_of_possible_value_types = CWL_OUTPUT_TYPE_TO_PYTHON_TYPE[output_type.output_category]
    if type(output_value) not in set_of_possible_value_types:
        if isinstance(output_value, dict):
            short_repr = 'dictionary'
        elif isinstance(output_value, list):
            short_repr = 'list'
        else:
            short_repr = 'value "{}" of type "{}"'.format(output_value, type(output_value).__name__)

        raise RedSpecificationError('Value should have type "{}", but found "{}".'.format(
            output_type.output_category.name, short_repr
        ))

    cli_type = output_type.output_category.name
    value_type = output_value.get('class')
    if cli_type != value_type:
        raise RedSpecificationError('Is declared as "{}" but given as "{}"'.format(
            cli_type, value_type
        ))


def _check_key_is_string(key, path):
    """
    Raises an RedSpecificationError, if the given key is not of type string.
    :param key: The key to check the type
    :param path: The path to this key
    :raise RedSpecificationError: If the given key has a type different from str
    """
    if not isinstance(key, str):
        if path:
            where = 'under "{}" '.format('.'.join(path))
        else:
            where = ''
        raise RedSpecificationError(
            'The key "{}" ({}) in REDFILE {}is not of type string'.format(key, type(key).__name__, where)
        )


def check_keys_are_strings(data, path=None):
    """
    Raises an RedSpecificationError, if a key is not of type string
    :param data: The data to check
    :param path: The path of keys as list of strings leading to data
    :raise RedSpecificationError: If a key is found, that has a type different from str
    """
    if path is None:
        path = []

    if isinstance(data, dict):
        for key, value in data.items():
            _check_key_is_string(key, path)
            check_keys_are_strings(value, path + [key])
    elif isinstance(data, list):
        for index, value in enumerate(data):
            check_keys_are_strings(value, path + [str(index)])


def convert_batch_experiment(red_data, batch):
    if 'batches' not in red_data:
        return red_data

    if batch is None:
        raise ArgumentError('batches are specified in REDFILE, but --batch argument is missing')

    try:
        batch_data = red_data['batches'][batch]
    except:
        raise ArgumentError('invalid batch index provided by --batch argument')

    result = {key: val for key, val in red_data.items() if not key == 'batches'}
    result['inputs'] = batch_data['inputs']

    if batch_data.get('outputs'):
        result['outputs'] = batch_data['outputs']

    return result
