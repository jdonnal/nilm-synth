#!/usr/bin/python3

# make sure all nilm_identify_load slices are aligned to a period (module 150 samples)

import asyncio
import numpy as np
import joule.api
import argparse
from nilm.filters.prep import Prep

######################################
### Run prep on an IV+sinefit dataset
######################################

prep_configs = {
    'nshift': 1,
    'nharm': 4,
    'current_indices': [2],
    'rotations': [0],
    'scale_factor': 120 / np.sqrt(2),
    'merge': True,
    'polar': False,
    'line_freq': 60,
    'samp_freq': 9000,
    'goertzel': True
}

async def main():
    node = joule.api.get_node()
    load_library = await node.folder_get("/Load Library")
    loads = get_pending_loads(load_library, "")
    for load in loads:
        await run_prep(node, load)
    await node.close()

async def run_prep(node, path):
    print("running prep for %s" % path)
    IV_stream = '/'.join((path,'IV'))
    ZC_stream  = '/'.join((path,'sinefit'))
    # create the prep stream
    prep_stream = create_prep_stream()
    prep_stream = await node.stream_create(prep_stream, path)
#    prep_stream = '/'.join((path,'prep'))
    iv_pipe = await node.data_read(IV_stream)
    zc_pipe = await node.data_read(ZC_stream)
    prep_pipe = await node.data_write(prep_stream)
    args = argparse.Namespace(**prep_configs)
    my_prep = Prep()
    await my_prep.run(args,
                      {'iv': iv_pipe,
                       'zero_crossings': zc_pipe},
                      {'prep': prep_pipe})
    await prep_pipe.close()
    
def create_prep_stream():
    return joule.api.Stream(name="prep", datatype="float32", elements=[
        joule.api.Element("P1","W"),
        joule.api.Element("Q1","W"),
        joule.api.Element("P3","W"),
        joule.api.Element("Q3","W"),
        joule.api.Element("P5","W"),
        joule.api.Element("Q5","W"),
        joule.api.Element("P7","W"),
        joule.api.Element("Q7","W")])
        
def get_pending_loads(folder: joule.api.Folder, path):
    my_path = '/'.join((path,folder.name))
    loads = []
    for child in folder.children:
        loads += get_pending_loads(child, my_path)
    streams = [s.name for s in folder.streams]
    if "IV" in streams and "prep" not in streams:
        loads += ['/'.join((path, folder.name))]
    return loads

if __name__=="__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
