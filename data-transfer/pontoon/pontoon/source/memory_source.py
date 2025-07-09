from datetime import datetime, timezone
from pontoon.base import Source, Namespace, Stream, Dataset, Progress, Mode



class MemorySource(Source):
    """ A Source implementation that can generates records from memory for testing and mocking sources """

    def __init__(self, config, cache_implementation, cache_config={}):
        self._config = config
        self._streams = []
        self._mode = config.get('mode')
        self._namespace = Namespace(config.get('connect', {}).get('namespace', 'memory'))
        self._dt = datetime.now(timezone.utc)
        self._batch_id = str(int(self._dt.timestamp()*1000))
        self._progress_callback = None
        self._cache = cache_implementation(self._namespace, cache_config)

    
    def close(self):
        self._cache.close()


    def test_connect(self):
        return True


    def inspect_streams(self):
        return [{
            'schema_name': 'pontoon', 
            'stream_name': 'pontoon_transfer_test', 
            'fields': [
                {'name': 'id', 'type': 'string'},
                {'name': 'created_at', 'type': 'timestamp'},
                {'name': 'updated_at', 'type': 'timestamp'},
                {'name': 'customer_id', 'type': 'string'},
                {'name': 'name', 'type': 'string'},
                {'name': 'email', 'type': 'string'},
                {'name': 'score', 'type': 'int32'},
                {'name': 'notes', 'type': 'string'}
            ]
        }]

    
    def read(self, progress_callback=None) -> Dataset:
        # read some static records from memory
        # could be extended to take options via config and generate streams/records dynamically
        
        if callable(progress_callback):
            self._progress_callback = progress_callback
        else:
            self._progress_callback = lambda *args, **kwargs: None

        stream = Stream(
            name='pontoon_transfer_test',
            schema_name='pontoon',
            primary_field='id',
            cursor_field='created_at',
            filters={'customer_id': 'Customer5'},
            schema=Stream.build_schema([
                ('id',str),
                ('created_at',datetime),
                ('updated_at',datetime),
                ('customer_id',str),
                ('name',str),
                ('email',str),
                ('score',int),
                ('notes',str)])
        )

        self._streams.append(stream)


        batch = [
            stream.to_record(r) for r in [
                ['1', datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer5', 'User1', 'user1@example.com', 1, 'Notes for User1'],
                ['2', datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer5', 'User2', 'user2@example.com', 1, 'Notes for User2'],
                ['3', datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer5', 'User3', 'user3@example.com', 1, 'Notes for User3'],
                ['4', datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer5', 'User4', 'user4@example.com', 1, 'Notes for User4'],
                ['5', datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer5', 'User5', 'user5@example.com', 1, 'Notes for User5'],
                ['6', datetime.fromisoformat('2025-01-02T00:00:00+00:00'), datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer5', 'User6', 'user6@example.com', 1, 'Notes for User6'],
                ['7', datetime.fromisoformat('2025-01-02T00:00:00+00:00'), datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer5', 'User7', 'user7@example.com', 1, 'Notes for User7'],
                ['8', datetime.fromisoformat('2025-01-02T00:00:00+00:00'), datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer5', 'User8', 'user8@example.com', 1, 'Notes for User8'],
                ['9', datetime.fromisoformat('2025-01-02T00:00:00+00:00'), datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer5', 'User9', 'user9@example.com', 1, 'Notes for User9'],
                ['10', datetime.fromisoformat('2025-01-02T00:00:00+00:00'), datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer5', 'User10', 'user10@example.com', 1, 'Notes for User10']
            ]
        ]

        if self._mode.type == Mode.INCREMENTAL:
            batch = [r for r in batch \
                if r.data[1] >= self._mode.start and r.data[1] < self._mode.end]

        self._cache.write(stream, batch)

        self._progress_callback(Progress(len(batch), 0))

        return Dataset(
            self._namespace, 
            self._streams, 
            self._cache,
            meta={'batch_id': self._batch_id, 'dt': self._dt}
        )