import typing
from typing import List, Callable
import click
import joule.api
import joule.errors
import pandas as pd

from nilm_synth.models.run import Run
import h5py
import numpy as np
from nilm_synth.models.library_types import LibraryLoad
from .build_nilmtk_data import build_nilmtk_data

if typing.TYPE_CHECKING:
    pass


def initialize_hdf_data(hdf_root, dataset_start_ts,
                        dataset_end_ts, num_phases=1):
    # one sample per line cycle, timestamps are UNIX us
    num_samples = round((dataset_end_ts - dataset_start_ts) * 60e-6)

    hdf_timestamp = hdf_root.create_dataset('timestamp', (num_samples,), dtype='i8')
    start_idx = 0
    start_ts = dataset_start_ts
    block_size = 60 * 60 * 60  # 1 hour blocks
    # add the timestamps
    while True:
        end_idx = min(num_samples, start_idx + block_size)
        block_samples = end_idx - start_idx
        end_ts = start_ts + (block_samples / 60) * 1e6
        hdf_timestamp[start_idx:end_idx] = np.linspace(start_ts, end_ts, block_samples, endpoint=False)
        start_idx = end_idx
        start_ts = end_ts
        if end_idx == num_samples:
            break
    hdf_root.create_dataset('data', shape=(num_samples, num_phases * 8),
                            dtype='f', fillvalue=0)


BLOCK_SIZE = 10000


def add_noise_hd5_data(hdf_root: h5py.File, noise_power):
    if noise_power == 0:
        return  # nothing to do
    for idx in range(0, len(hdf_root['data']), BLOCK_SIZE):
        block = hdf_root['data'][idx:idx + BLOCK_SIZE, :]
        noise = np.random.normal(scale=np.sqrt(noise_power), size=block.shape)
        hdf_root['data'][idx:idx + BLOCK_SIZE, :] += noise


async def add_baseline_hd5_data(hdf_root: h5py.File, stream_config: str, node: joule.api.BaseNode, start_ts, end_ts):
    if stream_config is None:
        return  # nothing to do
    (path, phase) = stream_config.split(':')
    if phase == "":
        phase = 'A'
    if phase == 'A':
        indices = np.s_[:, :8]
    elif phase == 'B':
        indices = np.s_[:, 8:16]
    elif phase == 'C':
        indices = np.s_[:, 16:24]
    else:
        raise ValueError("Invalid phase for baseline stream, must be [A|B|C]")
    try:
        pipe = await node.data_read(path, start_ts, end_ts)
    except joule.errors.ApiError:
        raise ValueError("Cannot find baseline stream [%s] on node [%s]" % (path, node.name))
    start_idx = 0
    print("====Adding Baseline Data====")
    bar_ctx = click.progressbar(length=len(hdf_root['data']))
    bar = bar_ctx.__enter__()
    while not pipe.is_empty():
        sdata = await pipe.read()
        pipe.consume(len(sdata))
        end_idx = len(sdata) + start_idx
        hdf_root['data'][start_idx:end_idx] = sdata['data'][indices]
        start_idx = end_idx
        bar.update(len(sdata))
    bar_ctx.__exit__(None, None, None)


async def add_runs_hd5_data(runs: List[Run],
                            dataset_start_ts,
                            dataset_end_ts,
                            get_load_data: Callable[[int], LibraryLoad],
                            node: joule.api.BaseNode,
                            hdf_group,
                            num_phases=1,
                            ):
    sorted_runs = sorted(runs, key=lambda run: run.start_ts, reverse=False)
    cached_load_data = {}
    for run in sorted_runs:
        # if run.load_id not in cached_load_data:
        #    cached_load_data[run.load_id] = get_load_data(run.load_id)
        # run.instantiated_load = cached_load_data[run.load_id]
        await run.execute(node, hdf_group, dataset_start_ts)
