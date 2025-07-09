from typing import List, Dict, Tuple, Generator, Any
from pontoon.base import Cache, Namespace, Stream, Record


class MemoryCache(Cache):
    """ A Cache implementation that holds records in memory """
    
    def __init__(self, namespace:Namespace, config:Dict[str,Any]={}):
        self._namespace = namespace
        self._config = config
        self._cache = {}

    def write(self, stream:Stream, records:List[Record]):
        if stream.name not in self._cache:
            self._cache[stream.name] = []
        self._cache[stream.name].extend(records)
    
    def read(self, stream:Stream) -> Generator[Record, None, None]:
        for record in self._cache.get(stream.name, []):
            yield record

    def close(self):
        self._cache = {}