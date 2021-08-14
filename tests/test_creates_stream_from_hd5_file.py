import unittest
import h5py
import joule.api

from joule.utilities import human_to_timestamp
from nilm_synth.builders import build_data_stream
import asyncio


class TestCreatesStreamFromHd5File(unittest.TestCase):

    def setUp(self):
        pass

    def test_creates_stream_from_hd5_file(self):
        async def test():
            node = joule.api.get_node("lambda")
            # stream must already be created

            stream = await node.data_stream_get("/Load Library/Unit Tests")
            # remove any existing data
            dataset_start_ts = human_to_timestamp("7:00 July 1 2021")
            dataset_end_ts = human_to_timestamp("7:15 July 1 2021")
            #await node.data_delete(stream,dataset_start_ts, dataset_end_ts)
            with h5py.File("test_runs.hd5", 'r') as f:
                pipe = await node.data_write(stream, dataset_start_ts, dataset_end_ts)
                await build_data_stream(f, pipe)
            await node.close()
        asyncio.run(test())
