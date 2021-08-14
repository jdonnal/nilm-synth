import pandas as pd
import numpy as np
import h5py
import yaml
import click
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from nilm_synth.parsers.parse_dataset import Dataset
    from nilm_synth.parsers.parse_metadata import Metadata

BLOCK_SIZE = 10000


def build_nilmtk_data(nilmtk_hdf: pd.HDFStore, nilmtk_group,
                      raw_hdf: h5py.File, tz: str):
    #print("\texporting NILMTK dataset")
    labels = [['power'] * 3, ['active', 'reactive', 'apparent']]
    column_labels = pd.MultiIndex.from_arrays(labels, names=('physical_quantity', 'type'))
    dataset_length = len(raw_hdf['data'])
    bar_ctx = click.progressbar(length=dataset_length)
    bar = bar_ctx.__enter__()
    for idx in range(0, len(raw_hdf['data']), BLOCK_SIZE):
        end_idx = min(idx + BLOCK_SIZE, dataset_length)
        active = raw_hdf['data'][idx:end_idx][:, 0]
        reactive = raw_hdf['data'][idx:end_idx][:, 1]
        apparent = np.sqrt(active ** 2 + reactive ** 2)
        df = pd.DataFrame(data=np.hstack((raw_hdf['timestamp'][idx:end_idx][:, None],
                                          active[:, None], reactive[:, None], apparent[:, None])))
        df.set_index(0, inplace=True)
        df.index = pd.to_datetime(df.index.values, unit='us', utc=True)
        df = df.tz_convert(tz)
        df.columns = column_labels
        df = df.resample('1S').mean()
        # if this sample overlaps with an existing sample ignore it
        if nilmtk_group in nilmtk_hdf.keys():
            last_ts = nilmtk_hdf[nilmtk_group].index[-1:]
            if df.index[0] == last_ts:
                df = df[1:]
        nilmtk_hdf.append(nilmtk_group, df)
        bar.update(BLOCK_SIZE)
    bar_ctx.__exit__(None, None, None)


def build_nilmtk_metadata(dataset: 'Dataset', metadata: 'Metadata',
                          load_configs, store: pd.HDFStore,
                          get_appliance_type: Callable[[int], str]):
    dataset_m = {
        'name': metadata.name,
        'subject': "Synthetic dataset produced by nilm-synth",
        'description': metadata.desc,
        'creators': metadata.author,
        'contact': metadata.contact,
        'number_of_buildings': 1,
        'timezone': dataset.timezone,
        'schema': "https://github.com/nilmtk/nilm_metadata/tree/v0.2"
    }
    meter_devices_m = {
        'power_meter': {
            'model': 'varies',
            'sample_period': 1,
            'max_sample_period': 4,
            'wireless': False,
            'measurements': [
                {
                    'physical_quantity': 'power',
                    'type': 'active'
                },
                {
                    'physical_quantity': 'power',
                    'type': 'reactive'
                },
                {
                    'physical_quantity': 'power',
                    'type': 'apparent'
                },
            ]
        }
    }
    dataset_m['meter_devices'] = meter_devices_m
    store.root._v_attrs.metadata = dataset_m

    site_meter = {
        'device_model': 'power_meter',
        'submeter_of': 0,
        'site_meter': True,
        'data_location': f'/building1/elec/meter1'
    }
    building_m = {
        'instance': 1,
        'elec_meters': {
            1: site_meter
        },
        'appliances': [],
    }
    # add all the submeters
    submeter_id = 2
    instance = 1
    for config in load_configs:
        building_m['elec_meters'][submeter_id] = {
            'device_model': 'power_meter',
            'submeter_of': 1,
            'data_location': f'/building1/elec/meter{submeter_id}'
        }
        building_m['appliances'].append({
            'original_name': config['name'],
            'type': get_appliance_type(config['load_id']),
            'instance': instance,
            'meters': [submeter_id],
            'dominant_appliance': True,
            'on_power_threshold': 5  # arbitrary, set this from a value in LibraryLoad?
        })
        instance += 1
        submeter_id += 1

    bldg_group = store._handle.create_group('/', 'building1')
    bldg_group._f_setattr('metadata', building_m)

    with open('metadata/dataset.yaml', 'w') as f:
        yaml.dump(dataset_m, f, default_flow_style=False)

    with open('metadata/meter_devices.yaml', 'w') as f:
        yaml.dump(meter_devices_m, f, default_flow_style=False)

    with open('metadata/building1.yaml', 'w') as f:
        yaml.dump(building_m, f, default_flow_style=False)
