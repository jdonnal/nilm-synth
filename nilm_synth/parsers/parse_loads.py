import functools
from typing import List, Callable

import numpy.random
from joule.utilities import timestamp_to_human
from numpy.random import default_rng
from nilm_synth.models.library_types import LibraryLoad, LibraryExemplar
from nilm_synth.models.run import Run


def parse_loads(loads_config,
                dataset_start_ts,
                dataset_end_ts,
                rng: numpy.random.Generator,
                get_instantiated_load: Callable[[int], LibraryLoad]) -> List[Run]:
    runs = []
    meter_id = 2  # aggregate power is on meter1
    for config in loads_config:
        # Name setting
        name = None
        if 'name' in config:
            name = config['name']
        # Load ID setting
        if 'load_id' not in config:
            raise ValueError("Load missing [load_id]")
        if type(config['load_id']) is not int:
            raise ValueError("Load [load_id] must be an integer")
        wrapped_get_instantiated_load = functools.partial(get_instantiated_load,
                                                          load_id=config['load_id'])
        # Scale Factor setting
        scale_factor = 1.0
        if 'scale_factor' in config:
            if ((type(config['scale_factor']) is not float) and
                    (type(config['scale_factor']) is not int)):
                raise ValueError("Load [scale_factor] must be a number")
            if config['scale_factor'] <= 0:
                raise ValueError("Load [scale_factor] must be positive")
            scale_factor = config['scale_factor']
        # Flex settings
        flex_on_pct = 0.05
        flex_off_pct = 0.10
        flex_power_pct = 0.01
        if 'flex' in config:
            flex_settings = config['flex']
            # no flexibility in loads
            if flex_settings == 'none':
                flex_on_pct = 0
                flex_off_pct = 0
                flex_power_pct = 0
            # use the specified flexibility settings
            if 'on_time' in flex_settings:
                flex_on_pct = _get_percentage(flex_settings['on_time'])
            if 'off_time' in flex_settings:
                flex_off_pct = _get_percentage(flex_settings['off_time'])
            if 'power' in flex_settings:
                flex_power_pct = _get_percentage(flex_settings['power'])
        # Run settings
        if 'runs' not in config:
            raise ValueError("Load [runs] missing")
        if type('runs') is not str:
            raise ValueError("Load [runs] invalid syntax")
        run_type = config['runs'].split(' ')[0]
        run_config = ' '.join(config['runs'].split(' ')[1:])
        if run_type == 'random':
            runs += _compute_random_runs(run_config, name,
                                         wrapped_get_instantiated_load, meter_id,
                                         scale_factor, flex_on_pct,
                                         flex_power_pct,
                                         dataset_start_ts, dataset_end_ts,
                                         rng)
        elif run_type == 'periodic':
            flex_off_pct = flex_on_pct  # TODO, add this as a configuration attribute
            runs += _compute_periodic_runs(run_config, name,
                                           wrapped_get_instantiated_load, meter_id,
                                           scale_factor, flex_on_pct, flex_off_pct,
                                           flex_power_pct,
                                           dataset_start_ts, dataset_end_ts,
                                           rng)
        elif run_type == 'fixed':
            runs += _compute_fixed_runs(run_config, name,
                                        wrapped_get_instantiated_load, meter_id,
                                        scale_factor, flex_on_pct,
                                        flex_power_pct, dataset_start_ts, dataset_end_ts,
                                        rng)
        else:
            raise ValueError("Load [runs] unsupported type, must be fixed|periodic|random")
        meter_id += 1
    return runs


def _compute_random_runs(run_config, name, get_instantiated_load: Callable[[], LibraryLoad],
                         meter_id,
                         scale_factor, flex_on_pct, flex_power,
                         dataset_start_ts, dataset_end_ts,
                         rng: numpy.random.Generator) -> List[Run]:
    values = run_config.split(':')
    count = int(values[0])
    target_duration = None
    if len(values) == 2:
        target_duration = _parse_time_str(values[1])
    runs = []
    for _ in range(count):
        num_steady_state_blocks = 0
        # get a new load instance with an exemplar for every run
        instantiated_load = get_instantiated_load()
        ex = instantiated_load.exemplar
        duration = ex.base_duration

        if target_duration is not None and not ex.has_steady_state:
            raise ValueError("Cannot specify duration for load with no steady state")
        if target_duration is not None:
            num_steady_state_blocks = _compute_num_steady_state_blocks(target_duration, ex)
        # compute the full duration
        if num_steady_state_blocks > 0:
            duration += num_steady_state_blocks * ex.steady_state_duration

        # generate a time flex std dev so that 95% of results are within the flex
        flex_on_time = flex_on_pct * duration
        time_padding = round(abs(rng.standard_normal(1)[0] * flex_on_time / 2))
        # generate a start time that does not conflict with other runs
        # and allows the exemplar to stop before the end of the dataset
        full_duration = duration + time_padding
        if full_duration > dataset_end_ts - dataset_start_ts:
            full_duration = duration  # remove time padding
            if full_duration > dataset_end_ts - dataset_start_ts:
                raise ValueError("Load duration is too long for dataset")
        start_ts = _find_valid_start_ts(full_duration, runs, dataset_start_ts, dataset_end_ts)

        # generate a scale_factor flex
        flex_scale_factor = (rng.standard_normal(1)[0] * flex_power / 2 + 1) * scale_factor
        runs.append(Run(name, start_ts, start_ts + duration + time_padding, instantiated_load,
                        meter_id, flex_scale_factor, time_padding, num_steady_state_blocks))
    return runs


def _compute_periodic_runs(run_config, name,
                           get_instantiated_load: Callable[[], LibraryLoad], meter_id,
                           scale_factor, flex_on_pct, flex_off_pct, flex_power,
                           dataset_start_ts, dataset_end_ts,
                           rng: numpy.random.Generator) -> List[Run]:
    values = run_config.split(':')
    period = _parse_time_str(values[0])
    target_duration = None
    if len(values) == 2:
        target_duration = _parse_time_str(values[1])
    runs = []
    current_ts = dataset_start_ts
    while current_ts < dataset_end_ts:
        num_steady_state_blocks = 0
        # get a new load instance with an exemplar for every run
        instantiated_load = get_instantiated_load()
        ex = instantiated_load.exemplar
        duration = ex.base_duration

        if target_duration is not None and not ex.has_steady_state:
            raise ValueError("Cannot specify duration for nilm_identify_load with no steady state")
        if target_duration is not None:
            num_steady_state_blocks = _compute_num_steady_state_blocks(target_duration, ex)
        # compute the full duration
        if num_steady_state_blocks > 0:
            duration += num_steady_state_blocks * ex.steady_state_duration

        # generate a time flex std dev so that 95% of results are within the flex
        flex_on_time = flex_on_pct * duration
        time_padding = round(abs(rng.standard_normal(1)[0] * flex_on_time / 2))
        # generate a start time that does not conflict with other runs
        # and allows the exemplar to stop before the end of the dataset
        full_duration = duration + time_padding

        flex_off_time = flex_off_pct * period
        wait_time = round(abs(rng.standard_normal(1)[0] * flex_off_time / 2)) + period

        start_ts = current_ts + flex_off_time
        end_ts = start_ts + full_duration
        # make sure the run can stop before the end of the dataset
        if end_ts >= dataset_end_ts:
            break
        current_ts = end_ts + wait_time
        # generate a scale_factor flex
        flex_scale_factor = (rng.standard_normal(1)[0] * flex_power / 2 + 1) * scale_factor
        runs.append(Run(name, start_ts, end_ts, instantiated_load,
                        meter_id, flex_scale_factor, time_padding, num_steady_state_blocks))
    return runs


def _compute_fixed_runs(runs_config, name,
                        get_instantiated_load: Callable[[], LibraryLoad], meter_id,
                        scale_factor, flex_on_pct, flex_power, dataset_start_ts, dataset_end_ts,
                        rng: numpy.random.Generator) -> List[Run]:
    runs = []
    for run_config in runs_config.split(','):

        values = run_config.split(':')
        start_ts = _parse_time_str(values[0]) + dataset_start_ts
        target_duration = None
        if len(values) == 2:
            target_duration = _parse_time_str(values[1])
        num_steady_state_blocks = 0
        # get a new load instance with an exemplar for every run
        instantiated_load = get_instantiated_load()
        ex = instantiated_load.exemplar
        duration = ex.base_duration

        if target_duration is not None and not ex.has_steady_state:
            raise ValueError("Cannot specify duration for nilm_identify_load with no steady state")
        if target_duration is not None:
            num_steady_state_blocks = _compute_num_steady_state_blocks(target_duration, ex)
        # compute the full duration
        if num_steady_state_blocks > 0:
            duration += num_steady_state_blocks * ex.steady_state_duration

        # generate a time flex std dev so that 95% of results are within the flex
        flex_on_time = flex_on_pct * duration
        time_padding = round(abs(rng.standard_normal(1)[0] * flex_on_time / 2))
        # generate a start time that does not conflict with other runs
        # and allows the exemplar to stop before the end of the dataset
        full_duration = duration + time_padding
        end_ts = start_ts + full_duration
        # make sure the run can stop before the end of the dataset
        if end_ts >= dataset_end_ts:
            raise ValueError("Fixed load run time extends passed the end of the dataset")
        # generate a scale_factor flex
        flex_scale_factor = (rng.standard_normal(1)[0] * flex_power / 2 + 1) * scale_factor
        runs.append(Run(name, start_ts, end_ts, instantiated_load,
                        meter_id, flex_scale_factor, time_padding, num_steady_state_blocks))
    return runs


def _find_valid_start_ts(duration, other_runs, dataset_start_ts, dataset_end_ts):
    MAX_TRIES = 30
    rng = default_rng()

    for _ in range(MAX_TRIES):
        start_ts = round(rng.uniform(dataset_start_ts, dataset_end_ts - duration, 1)[0])
        end_ts = start_ts + duration
        conflict = False
        for run in other_runs:
            if ((run.start_ts < start_ts < run.end_ts) or
                    (run.start_ts < end_ts < run.end_ts)):
                conflict = True
                break
        if not conflict:
            break
    else:
        raise ValueError("Could fit requested number of random runs for nilm_identify_load in dataset")
    return start_ts


def _parse_time_str(time_str: str):
    # supported units:
    # none = microseconds
    # s: seconds
    # m: minutes
    # h: hours
    try:
        return int(time_str)
    except ValueError:
        pass
    unit = time_str[-1]
    duration = float(time_str[:-1])
    if unit == 's':
        return int(duration * 1e6)
    elif unit == 'm':
        return int(duration * 1e6 * 60)
    elif unit == 'h':
        return int(duration * 1e6 * 60 * 60)
    else:
        raise ValueError("Unsupported duration unit [%s], must be s|m|h" % unit)


def _get_percentage(str_value: str):
    # turn 5% into 0.05
    try:
        if type(str_value) is not str:
            raise ValueError()
        if not str_value.endswith('%'):
            raise ValueError()
        pct_val = float(str_value[:-1])
        if pct_val < 0:
            raise ValueError()
    except ValueError:
        raise ValueError("flex values must be percentages such as 35%")

    return pct_val / 100.0


def _compute_num_steady_state_blocks(target_duration: int, exemplar: LibraryExemplar):
    # make sure the nilm_identify_load has a steady state
    if not exemplar.has_steady_state:
        raise ValueError("[%s] does not have a steady state, duration cannot be changed" % exemplar.name)
    # convert argument to microseconds
    target_steady_state_duration = target_duration - exemplar.base_duration
    # use an integer number of steady state blocks
    num_steady_state_blocks = round(target_steady_state_duration /
                                    exemplar.steady_state_duration)
    if num_steady_state_blocks < 0:
        raise ValueError("Load duration is too short")
    return num_steady_state_blocks
