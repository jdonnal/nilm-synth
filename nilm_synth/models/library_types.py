from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, BigInteger, String, Binary, ForeignKey
from dataclasses import dataclass
from typing import Optional
import numpy as np
from nilmidentify.models.load import Load

metadata = MetaData()


@dataclass
class LibraryExemplar:
    on_start: int = None
    on_end: int = None
    ss_start: int = None
    ss_end: int = None
    off_start: int = None
    off_end: int = None
    delta_bytes: bytes = None
    load_id: int = None
    id: Optional[int] = None

    def __post_init__(self):
        if self.delta_bytes is not None:
            self.delta = np.frombuffer(self.delta_bytes)
        self.delta = None

    def set_delta(self, data: np.ndarray):
        self.delta_bytes = data.tobytes()
        self.delta = data

    @property
    def has_steady_state(self):
        if (self.ss_start is not None
                and self.ss_end is not None):
            return True
        else:
            return False

    @property
    def base_duration(self):
        # with no steady state blocks
        return (self.on_end - self.on_start) + \
               (self.off_end - self.off_start)

    @property
    def steady_state_duration(self):
        if not self.has_steady_state:
            raise ValueError("Exemplar does not have a steady state")
        return self.ss_end - self.ss_start


@dataclass
class LibraryLoad:
    stream: str
    appliance_type: str
    name: str
    description: str
    image: str
    id: Optional[int] = None

    def __post_init__(self):
        # used in extract_exemplars
        self.nilm_identify_load: Optional[Load] = None
        # used in building runs
        self.exemplar: Optional[LibraryExemplar] = None


library_load_table = Table("loads", metadata,
                           Column("id", Integer, primary_key=True),
                           Column("stream", String),
                           Column("name", String),
                           Column("description", String),
                           Column("image", String),
                           Column("appliance_type", String))

library_exemplar_table = Table("exemplars", metadata,
                               Column("id", Integer, primary_key=True),
                               Column("on_start", BigInteger),
                               Column("on_end", BigInteger),
                               Column("ss_start", BigInteger),
                               Column("ss_end", BigInteger),
                               Column("off_start", BigInteger),
                               Column("off_end", BigInteger),
                               Column("delta_bytes", Binary),
                               Column("load_id",
                                      ForeignKey('loads.id', ondelete="CASCADE")))
