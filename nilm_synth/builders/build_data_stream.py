import numpy as np
import click
import joule
import h5py
from typing import List
from joule.api import BaseNode, Element, DataStreamInfo

from joule.models.pipes import compute_dtype, Pipe
from joule.errors import ApiError


async def build_output_pipe(stream_path: str, start_ts, end_ts, node, num_phases=1) -> Pipe:
    return await get_pipe(stream_path, node, start_ts, end_ts, num_phases)


async def get_pipe(stream_path: str, node: BaseNode, start_ts: int, end_ts: int, num_phases) -> Pipe:
    try:
        stream = await node.data_stream_get(stream_path)
    except ApiError:
        # create the stream
        chunks = stream_path.split('/')
        folder = '/'.join(chunks[:-1])
        name = chunks[-1]
        stream = joule.api.DataStream(name,
                                      datatype='float32',
                                      elements=_build_elements(num_phases))
        stream = await node.data_stream_create(stream, folder)
    return await node.data_write(stream, start_ts, end_ts)


async def write_stream_data(hdf: h5py.File, pipe: Pipe, block_size=10000):
    #print("====Writing Dataset to Joule====")
    bar_ctx = click.progressbar(length=len(hdf['data']))
    bar = bar_ctx.__enter__()
    for idx in range(0, len(hdf['data']), block_size):
        ts = hdf['timestamp'][idx:idx + block_size]
        data = hdf['data'][idx:idx + block_size]
        sdata = np.empty(len(ts), dtype=compute_dtype(pipe.layout))
        sdata['timestamp'] = ts
        sdata['data'] = data
        await pipe.write(sdata)
        bar.update(len(data))
    await pipe.close()
    bar_ctx.__exit__(None, None, None)

#async def write_event_data(runs: List[Run], node: BaseNode):
#    events = [r.to_event for r in runs]
#    await node.event_stream_write(event_stream, events)

def _build_elements(num_phases: int) -> List[Element]:
    elements = []
    if num_phases == 1:
        for i in [1, 3, 5, 7]:
            elements.append(Element(f'P{i}', 'W'))
            elements.append(Element(f'Q{i}', 'W'))
    else:
        raise Exception("Multi-phase not implemented")
    return elements
