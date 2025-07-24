from typing import List, Dict, Tuple, Generator, Any
from pontoon.base import Cache, Namespace, Stream, Record


class FileSystemCache(Cache):
    def __init__(self, namespace, config:Dict[str, Any]):
        # self.namespace = namespace
        # self.config = config
        # self.root_dir = config.get("root_dir", "/tmp/cache")
        # self.batch_size = config.get("batch_size", 1000)
        # os.makedirs(self.root_dir, exist_ok=True)
        # self._open_files = {}  # optional: keep open file handles if needed

        raise NotImplementedError("FileSystemCache")


    def _stream_path(self, stream):
        subdir = os.path.join(self.root_dir, self.namespace.name, stream.schema_name)
        os.makedirs(subdir, exist_ok=True)
        return os.path.join(subdir, f"{stream.name}.cache")

    def write(self, stream, records:List) -> int:
        path = self._stream_path(stream)
        count = 0

        with open(path, 'wb') as f:
            # Write a placeholder for total record count (we'll come back to this)
            f.write(pickle.dumps(-1))  

            for i in range(0, len(records), self.batch_size):
                batch = records[i:i+self.batch_size]
                # Write just the `.data` payloads
                payloads = [r.data for r in batch]
                pickle.dump(payloads, f)
                count += len(payloads)

            # Go back and write the actual record count at the beginning
            f.seek(0)
            f.write(pickle.dumps(count))

        return count

    def read(self, stream) -> Generator:
        path = self._stream_path(stream)

        with open(path, 'rb') as f:
            _ = pickle.load(f)  # skip the count
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