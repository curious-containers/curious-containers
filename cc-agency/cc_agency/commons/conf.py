import os
import jsonschema
from ruamel.yaml import YAML

from cc_agency.commons.schemas.conf import conf_schema

yaml = YAML(typ='safe')


class Conf:
    def __init__(self, conf_file_path):
        if not conf_file_path:
            conf_file_path = os.path.join('~', '.config', 'cc-agency.yml')

        conf_file_path = os.path.expanduser(conf_file_path)

        with open(conf_file_path) as f:
            self.d = yaml.load(f)

        jsonschema.validate(self.d, conf_schema)
