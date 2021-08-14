from dataclasses import dataclass


@dataclass
class Resources:
    library_database: str
    output_stream: str
    output_file: str
    baseline_node: str = ""
    library_node: str = ""
    output_node: str = ""


def parse_resources(config):
    return Resources(**config)
