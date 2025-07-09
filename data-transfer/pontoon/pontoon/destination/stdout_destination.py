from pontoon.base import Destination, Dataset, Progress


class StdoutDestination(Destination):
    """ A Destination implementation that writes records to stdout for debugging """

    def __init__(self, config):
        self._config = config
        self._limit = config.get('connect', {}).get('limit', 100)
        self._progress_callback = None

    def write(self, ds:Dataset, progress_callback=None):

        if callable(progress_callback):
            self._progress_callback = progress_callback
        else:
            self._progress_callback = lambda *args, **kwargs: None

        count = 0
        print(ds.namespace.name)
        print('---')
        for stream in ds.streams:
            print(f"{stream.schema_name} / {stream.name}")
            print(stream.schema)
            print("===")
            for record in ds.read(stream):
                print(f"    {record.data}")
                count += 1
                self._progress_callback(Progress(-1, count))
                if count >= self._limit:
                    break
            print('===')
        self._progress_callback(Progress(count, 0))

    def close(self):
        pass