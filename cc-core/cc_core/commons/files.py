import os
import stat
import sys
import json
import textwrap

from ruamel.yaml import YAML, YAMLError

JSON_INDENT = 4

yaml = YAML(typ='safe')
yaml.default_flow_style = False


WRITE_PERMISSIONS = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH


def load_and_read(location, var_name):
    """
    Reads a path or URL and parses this file as yaml/json.
    :param location: The location as local path or URL
    :param var_name:
    :return:
    """
    if not location:
        return None
    raw_data = load(location, var_name)
    return read(raw_data, var_name)


def load(location, var_name):
    try:
        with open(os.path.expanduser(location)) as f:
            return f.read()
    except OSError as e:
        raise OSError(
            'File "{}" for argument "{}" could not be loaded from file system. Failed with the following message:\n{}'
            .format(location, var_name, str(e))
        )


def read(raw_data, var_name):
    try:
        data = yaml.load(raw_data)
    except YAMLError as e:
        raise YAMLError(
            'data for argument "{}" is neither json nor yaml formatted. Failed with the following message:\n{}'
            .format(var_name, str(e))
        )

    if not isinstance(data, dict):
        raise IOError(
            'data for argument "{}" does not contain a dictionary.\ntype(data): "{}"'
            .format(var_name, type(data).__name__)
        )

    return data


def file_extension(dump_format):
    if dump_format == 'json':
        return dump_format
    if dump_format in ['yaml', 'yml']:
        return 'yml'
    raise IOError('invalid dump format "{}"'.format(dump_format))


def dump(stream, dump_format, file_name):
    if dump_format == 'json':
        with open(file_name, 'w') as f:
            json.dump(stream, f, indent=JSON_INDENT)
    elif dump_format in ['yaml', 'yml']:
        with open(file_name, 'w') as f:
            yaml.dump(stream, f)
    else:
        raise IOError('invalid dump format "{}"'.format(dump_format))


def dump_print(stream, dump_format, error=False):
    if dump_format == 'json':
        if error:
            print(json.dumps(stream, indent=JSON_INDENT), file=sys.stderr)
        else:
            print(json.dumps(stream, indent=JSON_INDENT))
    elif dump_format in ['yaml', 'yml']:
        if error:
            yaml.dump(stream, sys.stderr)
        else:
            yaml.dump(stream, sys.stdout)
    elif dump_format != 'none':
        raise IOError('invalid dump format "{}"'.format(dump_format))


def wrapped_print(blocks, error=False):
    if error:
        for block in blocks:
            print(textwrap.fill(block), file=sys.stderr)
    else:
        for block in blocks:
            print(textwrap.fill(block))
