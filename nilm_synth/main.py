#!/usr/bin/python3
import functools

import pandas as pd
import sqlalchemy.engine
from sqlalchemy import create_engine, select
import asyncio
import joule.api
import yaml
import click
import h5py
import os
import random
from numpy import random
from typing import Optional
from joule.models.pipes import Pipe
import joule.errors

from nilm_synth.parsers.parse_loads import parse_loads
from nilm_synth.parsers.parse_metadata import parse_metadata, default_metadata
from nilm_synth.parsers.parse_dataset import parse_dataset
from nilm_synth.parsers.parse_resources import parse_resources
from nilm_synth.builders import (
    initialize_hdf_data, add_baseline_hd5_data,
    build_nilmtk_metadata,
    build_nilmtk_data, add_runs_hd5_data, add_noise_hd5_data)
from nilm_synth.builders import build_output_pipe, write_stream_data
from nilm_synth.models.library_types import (
    LibraryLoad, library_load_table,
    LibraryExemplar, library_exemplar_table)
from nilm_synth.models.library_types import metadata as library_metadata

VERSION = 0.6

async def main(config, force):
    print(f"NILM-Synth Power Dataset Simulator v{VERSION}")
    # === Run Parsers on Config File ===
    print("parsing specification file...", end="")
    with open(config, 'r') as f:
        config = yaml.safe_load(f)
        rng = random.default_rng()
        # 1.) Parse metadata section if it is present
        if 'metadata' in config:
            metadata = parse_metadata(config['metadata'])
        else:
            metadata = default_metadata
        # 2.) Parse dataset section
        if 'dataset' not in config:
            raise ValueError("Config file missing [dataset] section")
        dataset = parse_dataset(config['dataset'])

        # 3.) Parse the resources section
        if 'resources' not in config:
            raise ValueError("Config file missing [resources] section")
        resources = parse_resources(config['resources'])
        engine = create_engine('sqlite:///%s' % resources.library_database)
        library_metadata.create_all(engine)
        conn = engine.connect()
        wrapped_get_instantiated_load = functools.partial(get_instantiated_load, conn=conn)
        wrapped_get_load_appliance_type = functools.partial(get_load_appliance_type, conn=conn)

        # 3.) Parse the loads section
        if 'loads' not in config:
            raise ValueError("Config file missing [loads] section")
        runs = parse_loads(config['loads'], dataset.start_ts,
                           dataset.end_ts, rng, wrapped_get_instantiated_load)
    # === Run Builders to Create Dataset ===
    library_node = joule.api.get_node(resources.library_node)
    output_node = joule.api.get_node(resources.output_node)
    baseline_node = joule.api.get_node(resources.baseline_node)

    # if the output resources already exist confirm their removal
    if os.path.isfile(resources.output_file):
        if not force:
            click.confirm("Output file [%s] exists, overwrite?" % resources.output_file, abort=True)
        os.remove(resources.output_file)
    try:
        await output_node.folder_get(resources.output_stream)
        if not force:
            click.confirm("Output Joule folder [%s] exists, overwrite?" % resources.output_stream, abort=True)
        await output_node.folder_delete(resources.output_stream)
    except joule.errors.ApiError:
        pass  # folder does not exist so nothing to remove

    output_pipe: Optional[Pipe] = None
    nilmtk_hdf = pd.HDFStore(resources.output_file)
    build_nilmtk_metadata(dataset, metadata, config['loads'], nilmtk_hdf,
                          wrapped_get_load_appliance_type)

    f: Optional[h5py.File] = None
    print("[OK]")
    try:

        # Create the HDF5 data file
        print("---running main simulation---")
        f = h5py.File(resources.output_file + '.raw', 'w')
        initialize_hdf_data(f, dataset.start_ts, dataset.end_ts)
        await add_baseline_hd5_data(f,
                                    dataset.baseline_stream,
                                    baseline_node,
                                    dataset.start_ts,
                                    dataset.end_ts)
        add_noise_hd5_data(f, dataset.noise)
        await add_runs_hd5_data(runs, dataset.start_ts,
                                dataset.end_ts, wrapped_get_instantiated_load,
                                library_node, hdf_group=f['data'])
        output_pipe = await build_output_pipe(resources.output_stream + "/main",
                                              dataset.start_ts,
                                              dataset.end_ts, output_node)
        print("  exporting data")
        await write_stream_data(f, output_pipe)
        await output_pipe.close()
        build_nilmtk_data(nilmtk_hdf, _nilmtk_hdf_group(1), f, dataset.timezone)
        # add individual nilm_identify_load streams
        meter_id = 2
        for config in config['loads']:
            print("\n---running submeter simulation---")
            f['data'][:] = 0
            load_path = resources.output_stream + "/" + config['name']
            submeter_runs = [r for r in runs if r.meter_id == meter_id]
            await add_runs_hd5_data(submeter_runs, dataset.start_ts,
                                    dataset.end_ts, wrapped_get_instantiated_load,
                                    library_node, hdf_group=f['data'])
            print("  exporting data")
            build_nilmtk_data(nilmtk_hdf, _nilmtk_hdf_group(meter_id), f, dataset.timezone)
            output_pipe = await build_output_pipe(load_path,
                                                  dataset.start_ts,
                                                  dataset.end_ts, output_node)
            await write_stream_data(f, output_pipe)
            await output_pipe.close()
            events = [r.to_event() for r in submeter_runs]
            event_stream = joule.api.EventStream(config['name'] + ' Events')
            #print(f"adding {config['name']} to {resources.output_stream}")
            event_stream = await output_node.event_stream_create(event_stream,
                                                                 resources.output_stream)
            await output_node.event_stream_write(event_stream, events)
            meter_id += 1

        # Use the HDF5 data file to create the Joule stream
        # with h5py.File(resources.output_file, 'r') as f:
        #    await write_stream_data(f, output_pipe)
    except OSError as e:
        if 'unable to lock file' in str(e):
            raise click.ClickException("Could not write hd5 data files. Are they open in another process?")
        else:
            raise e
    finally:
        await library_node.close()
        if output_pipe is not None:
            await output_pipe.close()
        await output_node.close()
        await baseline_node.close()
        nilmtk_hdf.close()
        if f is not None:
            f.close()
        nilmtk_hdf.close()


def get_instantiated_load(load_id: int, conn: sqlalchemy.engine.Engine) -> LibraryLoad:
    t = library_load_table
    query = select([t]).where(t.c.id == load_id)
    row = conn.execute(query).fetchone()
    load = LibraryLoad(**row)
    # pick a random exemplar
    t = library_exemplar_table
    query = select([t]).where(t.c.load_id == load_id)
    rows = conn.execute(query).fetchall()
    if len(rows) > 1:
        row = rows[random.randint(0, len(rows) - 1)]
    else:
        row = rows[0]
    load.exemplar = LibraryExemplar(**row)
    return load


def get_load_appliance_type(load_id: int, conn: sqlalchemy.engine.Engine) -> str:
    t = library_load_table
    query = select([t.c.appliance_type]).where(t.c.id == load_id)
    row = conn.execute(query).fetchone()
    return row[0]


def _nilmtk_hdf_group(meter_id: int):
    return f'/building1/elec/meter{meter_id}'


def _zero_dataset(data):
    BLOCK_SIZE = 10000
    start_idx = 0
    end_idx = BLOCK_SIZE
    while end_idx < len(data):
        data[start_idx:end_idx] = 0


@click.command()
@click.option("-c", "--config", help="YAML configuration file", required=True)
@click.option("-y", "--yes", "force",
              help="remove existing outputs without prompting", is_flag=True)
def run_main(config, force):
    # try:
    asyncio.run(main(config, force))


# except ValueError as e:
#    raise click.ClickException(str(e))


if __name__ == "__main__":
    run_main()
