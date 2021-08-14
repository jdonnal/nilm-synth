from dataclasses import dataclass
from joule.utilities import human_to_timestamp


@dataclass
class Dataset:
    start_ts: int
    end_ts: int
    timezone: str
    baseline_stream: str
    noise: float


def parse_dataset(config):
    # Start setting
    try:
        start_ts = human_to_timestamp(config['start'])
    except KeyError:
        raise Exception("Dataset missing [start] value")
    except ValueError:
        raise Exception("Dataset:start [%s] is not a recognized time format")
    # End setting
    try:
        end_ts = human_to_timestamp(config['end'])
    except KeyError:
        raise Exception("Dataset missing [end] value")
    except ValueError:
        raise Exception("Dataset:end [%s] is not a recognized time format")
    # Make sure duration makes sense
    if end_ts <= start_ts:
        raise Exception("Invalid dataset start/end values, must be a positive duration")
    # Baseline setting
    if 'baseline' in config:
        baseline_stream = config['baseline']
    else:
        baseline_stream = None
    # Noise setting
    if 'noise' in config:
        # must end with a w
        if config['noise'].lower()[-1] != 'w':
            raise Exception("Dataset:noise must be in watts (end with 'w')")
        try:
            noise = float(config['noise'][:-1])
            if noise < 0:
                raise ValueError()
        except ValueError:
            raise Exception("Dataset:noise must be a positive number (eg 3W)")
    else:
        noise = 0  # no noise
    # Timezone setting
    if 'timezone' in config:
        timezone = config['timezone']
    else:
        timezone = 'UTC'

    return Dataset(start_ts, end_ts, timezone,
                   baseline_stream,
                   noise)
