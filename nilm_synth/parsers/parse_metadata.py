from dataclasses import dataclass


@dataclass
class Metadata:
    name: str = "Test Data"
    desc: str = ""
    author: str = ""
    contact: str = ""
    misc: str = ""  # extra JSON data

def parse_metadata(config):
    metadata = Metadata(**config)
    return metadata

default_metadata = Metadata()
