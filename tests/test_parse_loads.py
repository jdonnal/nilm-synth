import unittest
import yaml
from nilm_synth.parsers.parse_loads import parse_loads
from nilm_synth.models import LibraryLoad
from numpy import random
import json
from joule.utilities import human_to_timestamp
from dataclasses import asdict


class TestParseLoads(unittest.TestCase):

    def setUp(self):
        pass

    def test_parses_single_random_load(self):
        config = yaml.safe_load("""
            loads:
              - name: Space Heater
                load_id: 2
                runs: random 2
              - name: Big Long Space Heater
                load_id: 2
                scale_factor: 2.0
                runs: random 3:20s
          """)
        # Space Heater from Load Library Dataset
        load_data = LibraryLoad(2, "/Load Library/Residential/Space Heater",
                                4832000, 18594000, 20260000, 22826000, 41453000, 43452000)

        def stub(load_id: int) -> LibraryLoad:
            self.assertEqual(load_id, 2)
            return load_data

        rng = random.default_rng(seed=1)

        runs = parse_loads(config['loads'],
                           dataset_start_ts=human_to_timestamp("7:00 July 1 2021"),
                           dataset_end_ts=human_to_timestamp("7:15 July 1 2021"),
                           get_instantiated_load=stub,
                           rng=rng)
        self.assertEqual(len(runs), 5)
        run_json = [asdict(r) for r in runs]

        with open('test_runs.json', 'w') as f:
            f.write(json.dumps(run_json, indent=4, sort_keys=True))
