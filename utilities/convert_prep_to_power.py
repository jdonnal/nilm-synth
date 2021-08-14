import click
import asyncio
import numpy as np
import pandas as pd

import joule.api
import joule.errors
from joule.api import BaseNode


async def main(folder, node: BaseNode, delete: bool):
    prep_stream = await node.data_stream_get(folder + '/prep')

    try:
        power_stream = await node.data_stream_get(folder + '/power')
        if delete:
            await node.data_delete(power_stream)  # clear any existing data
        else:
            # get the power data extents, if they match the source then skip
            power_info = await node.data_stream_info(power_stream)
            prep_info = await node.data_stream_info(prep_stream)

            if power_info.rows > 0:
                start_delta = abs(power_info.start - prep_info.start) / 1e6
                end_delta = abs(power_info.end - prep_info.end) / 1e6
                max_delta = max(start_delta, end_delta)
                if max_delta < 3:  # streams are within 3 seconds, consider it a match
                    print(f"stream is already converted, skipping")
                    await node.close()
                    return
                print("here!")
                if click.confirm(f"stream already has power data, remove it?"):
                    await node.data_delete(power_stream)  # clear any existing data
                else:
                    print(f"\tskipping {folder}")
                    await node.close()
                    return

    except joule.errors.ApiError:
        power_stream = joule.api.DataStream("power", elements=[
            joule.api.Element(name='active', units='W'),
            joule.api.Element(name='reactive', units='VAR'),
            joule.api.Element(name='apparent', units='VA'),
        ])
        power_stream = await node.data_stream_create(power_stream, folder)
    await asyncio.sleep(0.1)
    prep_pipe = await node.data_read(prep_stream)
    power_pipe = await node.data_write(power_stream)
    last_ts = None
    add_zero_sample = True
    try:
        while not prep_pipe.is_empty():
            data = await prep_pipe.read(flatten=True)
            apparent = np.sqrt(data[:, 1] ** 2 + data[:, 2] ** 2)
            prep_pipe.consume(len(data))
            df = pd.DataFrame(data=np.hstack((data[:, :3], apparent[:, None])))
            df.set_index(0, inplace=True)
            df.index = pd.to_datetime(df.index.values, unit='us', utc=True)
            df = df.resample('1S').mean()
            resampled_data = df.to_numpy()
            df.reset_index(inplace=True)
            resampled_timestamps = np.array([val.timestamp() * 1e6 for val in df['index']])
            result = np.hstack((resampled_timestamps[:, None], resampled_data))
            if add_zero_sample:  # first data block
                # start the data with zeros
                await power_pipe.write(np.array([[result[0, 0] - 1e6, 0, 0, 0]]))
                add_zero_sample = False
            if len(result) != 0:
                if last_ts == result[0, 0]:
                    result = result[1:, :]  # ignore the first sample since it is a duplicate timestamp
                last_ts = result[-1, 0]
                await asyncio.sleep(0.1)
                await power_pipe.write(result)

            if prep_pipe.end_of_interval:
                await power_pipe.write(np.array([[last_ts + 1e6, 0, 0, 0]]))
                add_zero_sample = True
                await power_pipe.close_interval()
            print(".", end="")
    except joule.errors.EmptyPipeError:
        pass
    await prep_pipe.close()
    await power_pipe.close()
    await node.close()
    print("[DONE]")


@click.command()
@click.argument("folder")
@click.option("-d", "--delete", is_flag=True, help="delete existing power data")
@click.option('-n', '--node')
def run_main(folder, node, delete):
    try:
        asyncio.run(main(folder, joule.api.get_node(node), delete))
    except ValueError as e:
        raise click.ClickException(str(e))


if __name__ == "__main__":
    run_main()
