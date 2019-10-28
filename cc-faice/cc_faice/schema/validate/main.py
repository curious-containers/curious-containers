import sys
import jsonschema
from argparse import ArgumentParser

from jsonschema import ValidationError

from cc_core.commons.exceptions import print_exception, exception_format, RedValidationError
from cc_core.commons.schema_map import schemas
from cc_core.commons.files import load_and_read, dump_print

DESCRIPTION = 'Validate data against schema. Returns code 0 if data is valid.'


def attach_args(parser):
    parser.add_argument(
        'schema', action='store', type=str, metavar='SCHEMA',
        help='SCHEMA as in "faice schemas list".'
    )
    parser.add_argument(
        'file', action='store', type=str, metavar='FILE',
        help='FILE (json or yaml) to be validated as local path or http url.'
    )
    parser.add_argument(
        '--format', action='store', type=str, metavar='FORMAT', choices=['json', 'yaml', 'yml'], default='yaml',
        help='Specify FORMAT for debug information [json, yaml, yml]. Default is yaml.'
    )
    parser.add_argument(
        '-d', '--debug', action='store_true',
        help='Write debug info, including detailed exceptions, to stdout.'
    )


def main():
    parser = ArgumentParser(description=DESCRIPTION)
    attach_args(parser)
    args = parser.parse_args()
    result = run(**args.__dict__)

    if args.debug:
        dump_print(result, args.format)

    if result['state'] != 'succeeded':
        return 1

    return 0


def run(schema, file, **_):
    result = {
        'state': 'succeeded',
        'debugInfo': None
    }

    try:
        if schema not in schemas:
            debug_info = 'Schema "{}" not found. Use "faice schema list" for available schemas.'.format(schema)
            print(debug_info, file=sys.stderr)
            result['debugInfo'] = debug_info
            result['state'] = 'failed'
            return result

        data = load_and_read(file, 'FILE')
        jsonschema.validate(data, schemas[schema])
    except ValidationError as e:
        where = '/'.join([str(s) for s in e.absolute_path]) if e.absolute_path else '/'
        debug_info = 'File "{}" does not comply with schema "{}":\n\tkey in red file: {}\n\treason: {}'\
            .format(file, schema, where, e.message)

        print_exception(RedValidationError(debug_info))
        result['debugInfo'] = exception_format()
        result['state'] = 'failed'

    except Exception as e:
        print_exception(e)
        result['debugInfo'] = exception_format()
        result['state'] = 'failed'

    return result
