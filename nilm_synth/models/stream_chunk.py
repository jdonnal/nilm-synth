from dataclasses import dataclass


@dataclass
class StreamChunk:
    start_ts: int
    end_ts: int
    phase: str
    delta: int = 0
