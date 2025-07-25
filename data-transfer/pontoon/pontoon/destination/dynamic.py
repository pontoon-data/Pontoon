from typing import List
from pontoon.base import Destination, Dataset


def create_multi_destination(destinations:List[Destination]=[]):

    class MultiDestination(Destination):
        """ 
            A virtual Destination that wraps multiple Destinations 
        
            The config object needs to be the union of settings for all destinations.
        """

        def __init__(self, config):

            connect = config.get('connect')
            self._target_schema = connect.get('target_schema')
            self._destinations = [dest_cls(config) for dest_cls in destinations]
        
        
        def integrity(self):
            return self._destinations[-1].integrity()


        def write(self, ds:Dataset, progress_callback=None):
            if self._target_schema:
                for stream in ds.streams:
                    ds.rename_stream(
                        stream.name, 
                        stream.schema_name,
                        stream.name,           # keep the existing table name
                        self._target_schema,   # new schema name
                    )

            for dest in self._destinations:
                dest.write(ds, progress_callback=progress_callback)
        
        def close(self):
            pass


    return MultiDestination

