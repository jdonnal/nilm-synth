import unittest
import json
import h5py
import joule.api
import numpy as np
from nilm_synth.models.run import from_json
from nilm_synth.models import LibraryLoad
from nilm_synth.builders import add_runs_hd5_data
from joule.utilities import human_to_timestamp
import asyncio


class TestBuildDataset(unittest.TestCase):

    def setUp(self):
        pass

    def test_builds_hd5_dataset_from_run_file(self):
        runs = []
        with open('tests/random_runs.json', 'r') as f:
            for item in json.loads(f.read()):
                runs.append(from_json(item))

        # Space Heater from Load Library Dataset
        load_data = LibraryLoad(2, "/Load Library/Residential/Space Heater",
                                4832000, 18594000, 20260000, 22826000, 41453000, 43452000)

        def stub(load_id: int) -> LibraryLoad:
            self.assertEqual(load_id, 2)
            return load_data

        dataset_start_ts = human_to_timestamp("7:00 July 1 2021")
        dataset_end_ts = human_to_timestamp("7:15 July 1 2021")
        node = joule.api.get_node('hollyberry')
        with h5py.File("test_runs.hd5", 'w') as f:
            asyncio.run(add_runs_hd5_data(runs, dataset_start_ts, dataset_end_ts,
                                          get_load_data=stub, node=node, hdf_root=f,
                                          num_phases=1))

        with h5py.File("test_runs.hd5", 'r') as f:
            # verify the timestamps cover the right range and are spaced 1 line cycle (60Hz) apart
            self.assertAlmostEqual(f['timestamp'][0], dataset_start_ts)
            # to within a line cycle
            self.assertAlmostEqual(f['timestamp'][-1] / 1e6, dataset_end_ts / 1e6, places=1)
            # upper and lower bounds of 1/60
            self.assertEqual(np.min(np.diff(f['timestamp'])), 16666)
            self.assertEqual(np.max(np.diff(f['timestamp'])), 16667)

        asyncio.run(node.close())
