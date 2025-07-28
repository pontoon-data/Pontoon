from pontoon.base import Destination, Dataset, Progress
from pontoon.destination.integrity import MockIntegrity

class StdoutDestination(Destination):
    """ A Destination implementation that writes records to stdout for debugging """

    def __init__(self, config):
        self._config = config
        self._limit = config.get('connect', {}).get('limit', 100)

    def integrity(self):
        return MockIntegrity()
    
    def write(self, ds:Dataset, progress_callback=None):

        print(ds.namespace.name)
        print('---')
        for stream in ds.streams:

            count = 0

            progress = Progress(
                f"{ds.namespace}/{stream.schema_name}/{stream.name}",
                total=ds.size(stream),
                processed=0
            )
            if callable(progress_callback):
                progress.subscribe(progress_callback)

            print(f"{stream.schema_name} / {stream.name}")
            print(stream.schema)
            print("===")
            for record in ds.read(stream):
                if count < self._limit:
                    print(f"    {record.data}")
                
                count += 1
                progress.update(1, increment=True)
            print('===')


    def close(self):
        pass