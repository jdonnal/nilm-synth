#!/usr/bin/python3
import asyncio
import argparse
import numpy as np

import joule
import joule.api
from nilm.filters import sinefit

# INPUT_FILE = "raw_data/space_heater_data.dat"
# OUTPUT_FILE = "preprocessed_data/space_heater_data.dat"
# OUTPUT_ZC_FILE = "preprocessed_data/space_heater_zc.dat"
CURRENT_OFFSET = -0.13584
VOLTAGE_OFFSET = -6.8176
LOAD_LIBRARY = '/Load Library'
DATA_DIR = '/opt/synology_nilm/Load_Library'

############################
### Ingest LabJack data into Joule and run sinefit
############################

async def preprocess(iv_pipe: joule.Pipe, source: str):
    with open(source, 'r') as src:
        date = src.readline()
        time = src.readline()
        ts = joule.utilities.human_to_timestamp(" ".join((date, time)))
        # skip metadata
        for i in range(6):
            src.readline()
        # write out the voltage, current
        dt = 1 / 9000 * 1e6  # us
        ts = 0
        for line in src:
            data = line.rstrip().split('\t')
            await iv_pipe.write(np.array([[int(ts), float(data[3]) - VOLTAGE_OFFSET, float(data[4]) - CURRENT_OFFSET]]))
            ts += dt
    await iv_pipe.close()

### NOT USED
async def log_iv_data(iv_pipe: joule.Pipe):
    with open(OUTPUT_FILE, 'w') as dest:
        while True:
            try:
                data = await iv_pipe.read(flatten=True)
                iv_pipe.consume(len(data))
                for row in data:
                    dest.write('%d,%f,%f\n' % (row[0], row[1], row[2]))
            except joule.api.errors.ApiError:
                break

### NOT USED
async def log_zc_data(zc_pipe: joule.Pipe):
    with open(OUTPUT_ZC_FILE, 'w') as dest:
        while True:
            try:
                data = await zc_pipe.read(flatten=True)
                zc_pipe.consume(len(data))
                for row in data:
                    dest.write('%d\n' % (row[0]))
            except joule.api.errors.ApiError:
                break

### NOT USED
async def log_zc_indexes(zc_pipe: joule.Pipe, iv_pipe: joule.Pipe):
    # go through iv_pipe and find the closest timestamps to the zc_pipe
    idx = 0
    iv_ts = None
    marked_idx = 0
    while True:
        try:
            data = await zc_pipe.read()
        except joule.api.errors.ApiError:
            break
        zc_pipe.consume(len(data))
        # print("got %d rows of zc" % len(data))
        for zc_ts in data['timestamp']:
            while True:
                # get some iv data
                if iv_ts is None or len(iv_ts) == 0:
                    data = await iv_pipe.read()
                    # print("got %d rows of iv" % len(data))
                    iv_ts = data['timestamp']
                # go through the iv data until we find a match
                for offset in range(len(iv_ts)):
                    if iv_ts[offset] >= zc_ts:
                        idx += offset
                        print(idx)
                        # print("%d => %d @ %d idx" % (iv_ts[offset], zc_ts, idx))
                        iv_ts = iv_ts[offset + 1:]
                        iv_pipe.consume(offset)
                        break
                else:
                    # ran out of IV data too early
                    print("ran out early %d" % len(iv_ts))
                    iv_pipe.consume(len(iv_ts))
                    iv_ts = None
                    idx += offset
                    continue
                break


async def create_streams(node: joule.api.BaseNode, load_name: str, folder: str):
    # make sure the streams don't exist yet
    iv_stream = joule.api.Stream("IV", datatype="float32", elements=[joule.api.Element("V", "V"),
                                                                     joule.api.Element("I", "A")])
    await node.stream_create(iv_stream, folder + "/" + load_name)
    sf_stream = joule.api.Stream("sinefit", datatype="float32",
                                 elements=[joule.api.Element("Frequency", "Hz", display_type='discrete'),
                                           joule.api.Element("Amplitude", "V", display_type='discrete'),
                                           joule.api.Element("Offset", "V", display_type='discrete')])
    await node.stream_create(sf_stream, folder + "/" + load_name)


async def main():
    node = joule.api.get_node()
    load_name = input("Load name: ")
    source = "/".join((DATA_DIR, input("File name: %s/"%DATA_DIR)))
    dest_folder = input("Destination: %s/"% LOAD_LIBRARY)
    if dest_folder != '':
        dest = "/".join((LOAD_LIBRARY, dest_folder))
    else:
        dest = LOAD_LIBRARY
    print("Upload [%s] to [ %s ]? " % (source, "/".join((dest, load_name))), end='')
    resp = input("(y/N) ")
    if resp != 'y':
        print("Cancelled")
        return
    print("OK, running...")

    # create the desired stream
    await create_streams(node, load_name, dest)

    iv_pipe = await node.data_write(dest + "/" + load_name + "/IV")
    sf_pipe = await node.data_write(dest + "/" + load_name + "/sinefit")

    iv_pipe2 = joule.LocalPipe('float32_2', loop=loop, name='iv_data')
    iv_pipe.enable_cache(1000)
    iv_pipe.subscribe(iv_pipe2)

    sinefitter = sinefit.Sinefit()
    args = argparse.Namespace(**{
        'v_index': 1,
        'frequency': 60,
        'min_freq': 50,
        'max_freq': 70,
        'min_amp': 100,
    })

    tasks = [
        preprocess(iv_pipe, source),
        # log_zc_indexes(zc_data, iv_data2),
        # log_iv_data(iv_data2),
        sinefitter.run(args,
                       {'iv': iv_pipe2},
                       {'zero_crossings': sf_pipe}),
    ]
    await asyncio.gather(*tasks)
    await node.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
