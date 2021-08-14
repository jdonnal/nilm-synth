import asyncio
import click
from sqlalchemy.engine import create_engine
from sqlalchemy import select
import sqlalchemy
import yaml
import joule.api
from typing import List, Optional
import numpy as np
from dataclasses import asdict

from joule.api import BaseNode, DataStreamInfo, DataStream
from joule.utilities import timestamp_to_human
import joule.errors

from nilm_synth.parsers.parse_exemplars_config import parse_exemplar_extraction_config
from nilm_synth.models.stream_chunk import StreamChunk
from nilm_synth.models.library_types import (
    LibraryLoad, LibraryExemplar, library_load_table,
    library_exemplar_table, metadata)

from nilmidentify import dbtools
from nilmidentify.models.dataset import Dataset
from nilmidentify.models.load import Load
from nilmidentify.models.transients.multiphase_transient import multiphase_transient_table
from nilmidentify.models.transients.single_phase_transient import single_phase_transient_table
from nilmidentify.models.transients.composite_transient import composite_transient_table

VERSION = 0.6

async def main(config_file):
    print(f"NILM-Synth Organic Exemplar Extractor v{VERSION}")

    # Given a dataset and nilm_identify_load name find all isolated examples and
    # extract them into the specified nilm_identify_load library stream
    # also create an records in the nilm_identify_load library database
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    extraction_config = parse_exemplar_extraction_config(config)

    # create all of the resources
    source_node = joule.api.get_node(extraction_config.source_node)
    library_node = joule.api.get_node(extraction_config.library_node)
    engine = create_engine('sqlite:///%s' % extraction_config.library_database)
    metadata.create_all(engine)
    library_db = engine.connect()
    conn_info = await source_node.db_connection_info()
    conn_info.port = extraction_config.source_node_database_port
    engine = sqlalchemy.create_engine(conn_info.to_dsn())
    dbtool = dbtools.DbTool(engine)
    dataset = dbtool.retrieve_dataset(extraction_config.dataset)
    with engine.connect() as connection:
        for library_load in extraction_config.loads:
            print(f"Processing {library_load.name}: ")
            load = dbtool.retrieve_load(name=library_load.name)
            if load is None:
                raise click.ClickException(f"Load {library_load.name} is not in {extraction_config.dataset}")
            library_load.nilm_identify_load = load
            await asyncio.sleep(0.1)
            stream_chunks = _find_isolated_runs(load, connection)
            print("")  # add a newline
            print(f"{len(stream_chunks)}/{len(load.events)} events isolated")
            source_stream = await source_node.data_stream_get(dataset.stream)
            print("Writing exemplars to library")
            exemplars = await _extract_exemplars(stream_chunks, library_load.stream, library_node, source_node,
                                                 source_stream)
            # save information into the library database
            _save_load_to_library(library_db, library_load)
            _save_exemplars_to_library(library_db, library_load.id, exemplars)
            print("")  # add newline between loads
    # clean up the resources
    library_db.close()
    await source_node.close()
    await library_node.close()


def _find_isolated_runs(load: Load, connection, tolerance=100):
    PADDING = int(0.25e6)  # time before and after the event
    on_events = [e for e in load.events if e.state == 'on']
    # make sure the only transients are those associated with this nilm_identify_load

    isolated_runs = []
    for e in on_events:
        if e.end_type is None or e.end_type == 'time':
            print('.', end="")
            continue  # only consider loads with a clear ending transient
        start_t_ids, phase = _get_single_phase_transient_ids(e.start_transient_id, e.start_type, connection)
        end_t_ids, phase = _get_single_phase_transient_ids(e.end_transient_id, e.end_type, connection, phase)
        all_transient_ids = _get_dataset_single_phase_transient_ids(load.dataset, e.start_ts - PADDING,
                                                                    e.end_ts + PADDING, phase, connection, tolerance)
        other_transient_ids = [t_id for t_id in all_transient_ids if t_id not in start_t_ids + end_t_ids]
        if len(other_transient_ids) > 0:
            print('.', end="")
            continue  # not an isolated transient
        isolated_runs.append(StreamChunk(e.start_ts - PADDING, e.end_ts + PADDING, phase))
        print('Y', end="")
    return isolated_runs


def _get_dataset_single_phase_transient_ids(dataset: Dataset, start_ts: int,
                                            end_ts: int, phase, connection, tolerance) -> List[int]:
    table = single_phase_transient_table
    # match dataset, phase, and either the start or end must be within the time bounds
    # and greater than tolerance watts
    query = select([table]) \
        .where(table.c.dataset_id == dataset.id) \
        .where(table.c.phase == phase) \
        .where(((table.c.start_ts >= start_ts) & (table.c.start_ts <= end_ts)) | \
               ((table.c.end_ts >= start_ts) & (table.c.end_ts <= end_ts)))
    rows = connection.execute(query).fetchall()
    return [r.id for r in rows if abs(r.start_data[0] - r.end_data[0]) > tolerance]


# return  transient id's and their phase
def _get_single_phase_transient_ids(transient_id: int,
                                    transient_type: str,
                                    connection,
                                    phase: Optional[str] = None) -> (List[int], str):
    if transient_type == 'singlephase':
        query = select([single_phase_transient_table]).where(
            single_phase_transient_table.c.id == transient_id)
        row = connection.execute(query).fetchone()
        return [transient_id], row.phase

    elif transient_type == 'multiphase':
        query = select([multiphase_transient_table]).where(
            multiphase_transient_table.c.id == transient_id)
        row = connection.execute(query).fetchone()
        if row.phase_a_id is not None:
            t_id = row.phase_a_id
            t_phase = 'A'
        elif row.phase_b_id is not None:
            t_id = row.phase_b_id
            t_phase = 'B'
        elif row.phase_c_id is not None:
            t_id = row.phase_c_id
            t_phase = 'C'
        else:
            assert False, "all multiphase phase_id's are null"
        if phase is not None:
            assert phase == t_phase, "all transients must be on the same phase"
        return [t_id], t_phase
    elif transient_type == 'composite':
        query = select([composite_transient_table]).where(
            composite_transient_table.c.id == transient_id)
        row = connection.execute(query).fetchone()
        if len(row.phase) > 1:
            t_type = 'multiphase'
        else:
            t_type = 'singlephase'
        t_ids = []
        for t_id in row.transient_ids:
            (new_t_ids, phase) = _get_single_phase_transient_ids(
                t_id, t_type, connection, phase)
            t_ids += new_t_ids
        return (t_ids, phase)
    else:
        assert False, f"unknown transient type {transient_type}"


async def _extract_exemplars(stream_chunks: List[StreamChunk], library_path: str, library_node: BaseNode,
                             source_node: BaseNode, source_stream: DataStream) -> List[LibraryExemplar]:
    info: DataStreamInfo = await _retrieve_library_stream_info(library_node, library_path, source_stream)
    if info.end is None:
        last_ts = 0
    else:
        last_ts = info.end + 60 * 1e6
    library_pipe = await library_node.data_write(library_path + '/prep')
    exemplars: List[LibraryExemplar] = []
    data = None  # appease type checker
    with click.progressbar(stream_chunks) as bar:
        for chunk in bar:
            exemplar = LibraryExemplar(on_start=last_ts)
            pipe = await source_node.data_read(source_stream, chunk.start_ts, chunk.end_ts)
            time_offset = None
            data_offset = None
            while not pipe.is_empty():
                data = await pipe.read(flatten=True)
                pipe.consume(len(data))
                if time_offset is None:
                    time_offset = data[0, 0] - last_ts
                if data_offset is None:
                    data_offset = np.array(data[0, 1:])

                data[:, 0] -= time_offset
                data[:, 1:] -= data_offset
                if chunk.phase == 'A':
                    idx = np.r_[:9]
                elif chunk.phase == 'B':
                    idx = np.r_[0, 9:17]
                elif chunk.phase == 'C':
                    idx = np.s_[0, 17:]
                else:
                    assert False, f"unknown phase {chunk.phase}"

                await library_pipe.write(data[:, idx])
                last_ts = data[-1, 0]
            await library_pipe.close_interval()
            exemplar.off_end = last_ts
            middle_ts = round(np.mean([exemplar.on_start, exemplar.off_end]))
            exemplar.on_end = middle_ts
            exemplar.off_start = middle_ts
            last_row = data[-1, idx]
            exemplar.set_delta(last_row[1:])
            exemplars.append(exemplar)
            last_ts += 1 * 60 * 1e6  # separate exemplars by 1 minute intervals
    await library_pipe.close()
    return exemplars


async def _retrieve_library_stream_info(node: BaseNode, path: str, reference_stream: DataStream) -> DataStreamInfo:
    try:
        stream = await node.data_stream_get(path + '/prep')
    except joule.errors.ApiError:
        # create a new stream based on the reference stream
        reference_stream.name = 'prep'
        reference_stream.elements = reference_stream.elements[:8]  # only keep 1 phase
        stream = await node.data_stream_create(reference_stream, path)

    return await node.data_stream_info(stream)


def _save_load_to_library(conn, load: LibraryLoad):
    t = library_load_table
    # see if this nilm_identify_load is already in the database
    query = select([t]).where(t.c.stream == load.stream)
    row = conn.execute(query).fetchone()
    if row is None:
        # this is a new nilm_identify_load save it to the database
        query = library_load_table.insert().values(**asdict(load))
        result = conn.execute(query)
        load.id = result.inserted_primary_key[0]
    else:
        load.id = row.id


def _save_exemplars_to_library(conn: sqlalchemy.engine.Connection,
                               load_id: int, exemplars: List[LibraryExemplar]):
    # only save the new exemplars
    t = library_exemplar_table
    for exemplar in exemplars:
        query = select([t]). \
            where(t.c.on_start == exemplar.on_start). \
            where(t.c.load_id == load_id)
        result = conn.execute(query).fetchone()
        exemplar.load_id = load_id
        if result is None:
            # this is new so insert it
            query = t.insert().values(**asdict(exemplar))
            conn.execute(query)


@click.command()
@click.option("-c", "--config", help="YAML configuration file", required=True)
def run_main(config):
    asyncio.run(main(config))
