import pytest
import subprocess


def test_faice_exec_yaml_return_0():
    return_code = subprocess.call(["python3", "../cc-faice/cc_faice", "exec", "red/minimal.red"], shell=True)
    assert return_code == 0


def test_faice_exec_json_return_0():
    return_code = subprocess.call(["python3", "../cc-faice/cc_faice", "exec", "red/minimal.json"], shell=True)
    assert return_code == 0


def test_faice_convert_format():
    stdout = subprocess.check_output(["python3", "../cc-faice/cc_faice", "convert", "format", "red/minimal.json"])
    
    with open('red/minimal.red', 'r') as file:
        result = file.read()
    result = result.encode()
    
    assert stdout == result


def test_faice_convert_cwl():
    stdout = subprocess.check_output(["python3", "../cc-faice/cc_faice", "convert", "cwl", "red/minimal.red"])
    
    with open('red/minimal.cwl', 'r') as file:
        result = file.read()
    result = result.encode()
    
    assert stdout == result

