import uuid
from typing import List, Dict, Tuple, Generator, Any
from pontoon.base import Cache, Namespace, Stream, Record


class FileSystemCache(Cache):
    def __init__(self, namespace, config:Dict[str, Any]):
        self.namespace = namespace
        self.config = config
        self.unique_id = uuid.uuid4()
        self.root_dir = config.get("root_dir", "/tmp/cache")
        self.batch_size = config.get("batch_size", 10000) # aiming for a 1MB write
        
        os.makedirs(self.root_dir, exist_ok=True)
        self._files = {}

    
    def _stream_path(self, stream):
        streamdir = os.path.join(self.root_dir, self.unique_id, self.namespace.name, stream.schema_name)
        os.makedirs(streamdir, exist_ok=True)
        return os.path.join(streamdir, f"{stream.name}.cache")

    
    def write(self, stream, records:List) -> int:
        path = self._stream_path(stream)
        count = 0

        if path in self._files:
            fp = self._files[path]
        else:
            fp = open(path, 'wb')
            self._files[path] = fp

        for i in range(0, len(records), self.batch_size):
            batch = records[i:i+self.batch_size]
            # Write just the `.data` payloads
            payloads = [r.data for r in batch]
            pickle.dump(payloads, fp)

    def read(self, stream) -> Generator:
        path = self._stream_path(stream)

        with open(path, 'rb') as fp:
            while True:
                try:
                    batch = pickle.load(f)
                    for item in batch:
                        yield Record(item)
                except EOFError:
                    break

    def size(self, stream) -> int:
        path = self._stream_path(stream)
        with open(path, 'rb') as f:
            return pickle.load(f)

    def close(self):
        self._open_files.clear()