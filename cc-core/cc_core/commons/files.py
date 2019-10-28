import os
import stat
import sys
import json
import tarfile
import textwrap

from ruamel.yaml import YAML, YAMLError

from cc_core.commons.exceptions import AgentError

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
    except:
        raise AgentError('File "{}" for argument "{}" could not be loaded from file system'.format(location, var_name))


def read(raw_data, var_name):
    try:
        data = yaml.load(raw_data)
    except YAMLError as e:
        raise AgentError('data for argument "{}" is neither json nor yaml formatted. Failed with the following message:'
                         '\n{}'.format(var_name, str(e)))

    if not isinstance(data, dict):
        raise AgentError('data for argument "{}" does not contain a dictionary.\ndata: "{}"'.format(var_name, data))

    return data


def file_extension(dump_format):
    if dump_format == 'json':
        return dump_format
    if dump_format in ['yaml', 'yml']:
        return 'yml'
    raise AgentError('invalid dump format "{}"'.format(dump_format))


def dump(stream, dump_format, file_name):
    if dump_format == 'json':
        with open(file_name, 'w') as f:
            json.dump(stream, f, indent=JSON_INDENT)
    elif dump_format in ['yaml', 'yml']:
        with open(file_name, 'w') as f:
            yaml.dump(stream, f)
    else:
        raise AgentError('invalid dump format "{}"'.format(dump_format))


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
        raise AgentError('invalid dump format "{}"'.format(dump_format))


def wrapped_print(blocks, error=False):
    if error:
        for block in blocks:
            print(textwrap.fill(block), file=sys.stderr)
    else:
        for block in blocks:
            print(textwrap.fill(block))


def create_directory_tarinfo(directory_name, owner_name, owner_id=1000):
    """
    Creates a tarfile.TarInfo object, that represents a directory with the given directory name.
    The owner of the file directory has read, write, execute privileges for the created directory TarInfo object.

    :param directory_name: The name of the directory represented by the created TarInfo
    :type directory_name: str
    :param owner_name: The name of the owner of the directory
    :type owner_name: str
    :param owner_id: The id of the owner of the directory
    :type owner_id: int
    :return: A TarInfo object representing a directory with the given name
    :rtype: tarfile.TarInfo
    """
    directory_tarinfo = tarfile.TarInfo(directory_name)

    directory_tarinfo.type = tarfile.DIRTYPE
    directory_tarinfo.uid = owner_id
    directory_tarinfo.gid = owner_id
    directory_tarinfo.uname = owner_name
    directory_tarinfo.gname = owner_name
    directory_tarinfo.mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR

    return directory_tarinfo
