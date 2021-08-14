import unittest
import yaml
from nilm_synth.parsers.parse_exemplars_config import parse_exemplar_extraction_config
from nilm_synth.models import LibraryLoad
from numpy import random
import json
from joule.utilities import human_to_timestamp
from dataclasses import asdict
import os

CONFIG_FILE = os.path.join(os.path.dirname(__file__),
                           'organic_exemplar_configs/donnal_august.yml')


class TestParseExemplarConfig(unittest.TestCase):

    def test_parses_exemplar_config(self):
        with open(CONFIG_FILE, 'r') as f:
            config = yaml.safe_load(f)
        # Space Heater from Load Library Dataset
        exemplar_config = parse_exemplar_extraction_config(config)
        print(exemplar_config)
