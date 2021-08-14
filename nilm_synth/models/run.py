from dataclasses import dataclass
import numpy as np
import sys
import asyncio
from typing import Optional, TYPE_CHECKING
import joule.api

if TYPE_CHECKING:
    from nilm_synth.models.library_types import LibraryLoad


@dataclass
class Run:
    name: str
    start_ts: int
    end_ts: int
    instantiated_load: 'LibraryLoad'
    meter_id: int
    scale_factor: float
    time_padding: int
    steady_state_blocks: int

    def __post_init__(self):
        # set this before calling the execute method
        # an instantiated load is a load with an exemplar set
        # self.instantiated_load: Optional['LibraryLoad'] = None
        self.max_power = 0
        self.avg_power = 0
        self.power_acc = 0
        self.total_samples = 0
        self.energy = 0

    def __str__(self):
        return f"{self.start_ts},{self.instantiated_load.name},{self.scale_factor},{self.time_padding},{self.steady_state_blocks}"

    async def execute(self, node, data_array, dataset_start_ts):
        self._reset_stats()
        if self.instantiated_load is None:
            raise Exception("Must set instantiated_load before calling execute")
        ex = self.instantiated_load.exemplar
        delay = self.start_ts - dataset_start_ts
        idx = round(delay / ((1 / 60) * 1e6))
        # add the nilm_identify_load data to the array
        print("\t%s:" % self.name, end="")
        sys.stdout.flush()
        idx, last_value = await self._add_data(ex.on_start, ex.on_end, node, data_array, offset=idx)
        # apply time padding
        pad_width = round(self.time_padding / ((1 / 60) * 1e6))
        data_array[idx:(idx + pad_width), :] += last_value
        idx += pad_width
        for _ in range(self.steady_state_blocks):
            idx, last_row = await self._add_data(ex.ss_start, ex.ss_end, node, data_array, offset=idx)
        await self._add_data(ex.off_start, ex.off_end, node, data_array, offset=idx)
        print("[done]")
        self.avg_power = self.power_acc / self.total_samples
        self.energy = self.power_acc * (1 / 60)

    def _reset_stats(self):
        self.power_acc = 0
        self.total_samples = 0
        self.max_power = 0

    async def _add_data(self, start, end, node, output_array, offset):
        assert start is not None
        assert end is not None

        pipe = await node.data_read(self.instantiated_load.stream + "/prep", start, end)
        total_rows = 0
        last_value = None
        while not pipe.is_empty():
            sdata = await pipe.read()
            if len(sdata) == 0:
                continue
            await asyncio.sleep(0.1)
            data = sdata['data'] * self.scale_factor
            pipe.consume(len(sdata))
            total_rows += len(data)
            try:

                output_array[offset:(offset + len(data)), :] += data
                self.power_acc += np.sum(data[:, 0])
                self.max_power = max(self.max_power, np.max(data[:, 0]))
            except ValueError as e:
                print(e)
                breakpoint()
                print("oh no :)")
            self.total_samples += len(data)
            offset += len(data)
            print(".", end="")
            sys.stdout.flush()
            last_value = np.mean(data[-10:, :])

        return offset, last_value

    def to_event(self):
        return joule.api.Event(self.start_ts, self.end_ts, content={
            'max power (W)': float(self.max_power),
            'average power (W)': float(self.avg_power),
            'energy (J)': float(self.energy)
        })


def from_json(json_val: dict) -> Run:
    return Run(
        name=json_val['name'],
        meter_id=json_val['meter_id'],
        start_ts=json_val['start_ts'],
        end_ts=json_val['end_ts'],
        load_id=None,  # TODO
        scale_factor=json_val['scale_factor'],
        time_padding=json_val['time_padding'],
        steady_state_blocks=json_val['steady_state_blocks'])
