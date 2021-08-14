from dataclasses import dataclass
from typing import Optional, List
from nilm_synth.models.library_types import LibraryLoad, LibraryExemplar


@dataclass
class ExemplarExtractionConfig:
    source_node: str
    library_node: str
    dataset: str
    library_database: str
    loads: List[LibraryLoad]
    source_node_database_port: int = 5432


def parse_exemplar_extraction_config(config_dict) -> ExemplarExtractionConfig:
    loads_config_dict = config_dict['loads']
    config_dict['loads'] = []

    config = ExemplarExtractionConfig(**config_dict)
    for load_dict in loads_config_dict:
        load_dict['stream'] = load_dict['load_library_path']
        del load_dict['load_library_path']
        config.loads.append(LibraryLoad(**load_dict))
    return config
