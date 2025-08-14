import datetime
from datetime import timezone
from pontoon.base import Source, Namespace, Stream, Dataset, Progress, Mode



class MemorySource(Source):
    """ A Source implementation that can generates records from memory for testing and mocking sources """

    def __init__(self, config, cache_implementation, cache_config={}):
        self._config = config
        self._streams = []
        self._mode = config.get('mode')
        self._with = config.get('with', {})
        self._namespace = Namespace(config.get('connect', {}).get('namespace', 'memory'))
        self._dt = datetime.datetime.now(timezone.utc)
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
                {'name': 'total', 'type': 'float64'},
                {'name': 'open_date', 'type': 'date32'},
                {'name': 'prefs', 'type': 'str'},
                {'name': 'notes', 'type': 'string'}
            ]
        }]

    
    def read(self, progress_callback=None) -> Dataset:
        # read some static records from memory
        # could be extended to take options via config and generate streams/records dynamically

        stream = Stream(
            name='pontoon_transfer_test',
            schema_name='pontoon',
            primary_field='id',
            cursor_field='created_at',
            filters={'customer_id': 'Customer5'},
            schema=Stream.build_schema([
                ('id',str),
                ('created_at',datetime.datetime),
                ('updated_at',datetime.datetime),
                ('customer_id',str),
                ('name',str),
                ('email',str),
                ('score',int),
                ('total', float),
                ('open_date', datetime.date),
                ('prefs', dict),
                ('notes',str)])
        )

        # add bookkeeping columns to stream if configured
        if self._with.get('batch_id'):
            stream.with_batch_id(self._batch_id)
        if self._with.get('checksum'):
            stream.with_checksum()
        if self._with.get('version'):
            stream.with_version(self._with.get('version'))
        if self._with.get('last_sync'):
            stream.with_last_synced_at(self._dt)

        # ignore any stream fields?
        for field in self._config.get('streams', [{}])[0].get('drop_fields', []):
            stream.drop_field(field)

        self._streams.append(stream)


        batch = [
                ['1', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer3', 'User1', 'user1@example.com', 9, 64.66, datetime.date(2024, 3, 31), {'theme': 'dark', 'notifications': False}, 'Notes for User1'],
                ['2', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer2', 'User2', 'user2@example.com', 8, 79.63, datetime.date(2024, 12, 29), {'theme': 'light', 'notifications': False}, 'Notes for User2'],
                ['3', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer3', 'User3', 'user3@example.com', 10, 20.63, datetime.date(2024, 2, 18), {'theme': 'dark', 'notifications': False}, 'Notes for User3'],
                ['4', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-04T00:00:00+00:00'), 'Customer2', 'User4', 'user4@example.com', 7, 18.1, datetime.date(2024, 9, 2), {'theme': 'light', 'notifications': False}, 'Notes for User4'],
                ['5', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer1', 'User5', 'user5@example.com', 4, 17.56, datetime.date(2024, 8, 18), {'theme': 'light', 'notifications': False}, 'Notes for User5'],
                ['6', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-04T00:00:00+00:00'), 'Customer3', 'User6', 'user6@example.com', 10, 63.73, datetime.date(2024, 9, 28), {'theme': 'dark', 'notifications': False}, 'Notes for User6'],
                ['7', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer2', 'User7', 'user7@example.com', 2, 44.05, datetime.date(2024, 4, 20), {'theme': 'dark', 'notifications': True}, 'Notes for User7'],
                ['8', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer2', 'User8', 'user8@example.com', 8, 41.91, datetime.date(2024, 5, 22), {'theme': 'dark', 'notifications': False}, 'Notes for User8'],
                ['9', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer3', 'User9', 'user9@example.com', 9, 34.32, datetime.date(2024, 2, 29), {'theme': 'light', 'notifications': False}, 'Notes for User9'],
                ['10', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer2', 'User10', 'user10@example.com', 10, 97.53, datetime.date(2024, 9, 12), {'theme': 'dark', 'notifications': True}, 'Notes for User10'],
                ['11', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer1', 'User11', 'user11@example.com', 2, 61.3, datetime.date(2024, 8, 23), {'theme': 'light', 'notifications': False}, 'Notes for User11'],
                ['12', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer2', 'User12', 'user12@example.com', 5, 66.97, datetime.date(2024, 6, 12), {'theme': 'light', 'notifications': False}, 'Notes for User12'],
                ['13', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer3', 'User13', 'user13@example.com', 1, 59.07, datetime.date(2024, 10, 21), {'theme': 'dark', 'notifications': True}, 'Notes for User13'],
                ['14', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-05T00:00:00+00:00'), 'Customer3', 'User14', 'user14@example.com', 1, 80.21, datetime.date(2024, 10, 11), {'theme': 'dark', 'notifications': False}, 'Notes for User14'],
                ['15', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer1', 'User15', 'user15@example.com', 10, 64.16, datetime.date(2024, 3, 10), {'theme': 'light', 'notifications': False}, 'Notes for User15'],
                ['16', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-05T00:00:00+00:00'), 'Customer1', 'User16', 'user16@example.com', 2, 53.55, datetime.date(2024, 10, 30), {'theme': 'dark', 'notifications': False}, 'Notes for User16'],
                ['17', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer1', 'User17', 'user17@example.com', 9, 10.62, datetime.date(2024, 12, 26), {'theme': 'dark', 'notifications': False}, 'Notes for User17'],
                ['18', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer3', 'User18', 'user18@example.com', 5, 24.21, datetime.date(2024, 12, 2), {'theme': 'light', 'notifications': False}, 'Notes for User18'],
                ['19', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer1', 'User19', 'user19@example.com', 3, 84.07, datetime.date(2024, 1, 17), {'theme': 'light', 'notifications': False}, 'Notes for User19'],
                ['20', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer2', 'User20', 'user20@example.com', 3, 92.48, datetime.date(2024, 5, 30), {'theme': 'dark', 'notifications': False}, 'Notes for User20'],
                ['21', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer2', 'User21', 'user21@example.com', 10, 71.66, datetime.date(2024, 6, 1), {'theme': 'dark', 'notifications': True}, 'Notes for User21'],
                ['22', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer2', 'User22', 'user22@example.com', 10, 96.14, datetime.date(2024, 8, 19), {'theme': 'dark', 'notifications': True}, 'Notes for User22'],
                ['23', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer2', 'User23', 'user23@example.com', 5, 98.75, datetime.date(2024, 1, 19), {'theme': 'light', 'notifications': False}, 'Notes for User23'],
                ['24', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer1', 'User24', 'user24@example.com', 6, 53.37, datetime.date(2024, 7, 6), {'theme': 'dark', 'notifications': True}, 'Notes for User24'],
                ['25', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer3', 'User25', 'user25@example.com', 1, 43.81, datetime.date(2024, 6, 26), {'theme': 'light', 'notifications': False}, 'Notes for User25'],
                ['26', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-05T00:00:00+00:00'), 'Customer3', 'User26', 'user26@example.com', 8, 96.87, datetime.date(2024, 1, 28), {'theme': 'dark', 'notifications': True}, 'Notes for User26'],
                ['27', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-05T00:00:00+00:00'), 'Customer3', 'User27', 'user27@example.com', 5, 31.58, datetime.date(2024, 6, 29), {'theme': 'dark', 'notifications': False}, 'Notes for User27'],
                ['28', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-04T00:00:00+00:00'), 'Customer1', 'User28', 'user28@example.com', 7, 16.14, datetime.date(2024, 1, 13), {'theme': 'dark', 'notifications': True}, 'Notes for User28'],
                ['29', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer3', 'User29', 'user29@example.com', 3, 58.45, datetime.date(2024, 6, 10), {'theme': 'dark', 'notifications': False}, 'Notes for User29'],
                ['30', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer3', 'User30', 'user30@example.com', 4, 18.52, datetime.date(2024, 7, 24), {'theme': 'dark', 'notifications': True}, 'Notes for User30'],
                ['31', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-04T00:00:00+00:00'), 'Customer1', 'User31', 'user31@example.com', 5, 54.72, datetime.date(2024, 8, 2), {'theme': 'dark', 'notifications': False}, 'Notes for User31'],
                ['32', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer1', 'User32', 'user32@example.com', 1, 89.11, datetime.date(2024, 8, 14), {'theme': 'dark', 'notifications': True}, 'Notes for User32'],
                ['33', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer1', 'User33', 'user33@example.com', 7, 91.84, datetime.date(2024, 8, 20), {'theme': 'light', 'notifications': False}, 'Notes for User33'],
                ['34', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer2', 'User34', 'user34@example.com', 9, 84.8, datetime.date(2024, 9, 30), {'theme': 'light', 'notifications': False}, 'Notes for User34'],
                ['35', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-04T00:00:00+00:00'), 'Customer3', 'User35', 'user35@example.com', 10, 80.43, datetime.date(2024, 1, 17), {'theme': 'light', 'notifications': False}, 'Notes for User35'],
                ['36', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-05T00:00:00+00:00'), 'Customer3', 'User36', 'user36@example.com', 6, 90.63, datetime.date(2024, 7, 10), {'theme': 'light', 'notifications': True}, 'Notes for User36'],
                ['37', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer1', 'User37', 'user37@example.com', 7, 77.41, datetime.date(2024, 3, 30), {'theme': 'light', 'notifications': True}, 'Notes for User37'],
                ['38', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer2', 'User38', 'user38@example.com', 3, 46.25, datetime.date(2024, 12, 12), {'theme': 'light', 'notifications': True}, 'Notes for User38'],
                ['39', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer2', 'User39', 'user39@example.com', 6, 63.13, datetime.date(2024, 7, 7), {'theme': 'dark', 'notifications': False}, 'Notes for User39'],
                ['40', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-05T00:00:00+00:00'), 'Customer3', 'User40', 'user40@example.com', 10, 27.86, datetime.date(2024, 12, 19), {'theme': 'dark', 'notifications': False}, 'Notes for User40'],
                ['41', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer2', 'User41', 'user41@example.com', 2, 61.47, datetime.date(2024, 12, 2), {'theme': 'light', 'notifications': False}, 'Notes for User41'],
                ['42', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer3', 'User42', 'user42@example.com', 10, 47.69, datetime.date(2024, 10, 5), {'theme': 'light', 'notifications': False}, 'Notes for User42'],
                ['43', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer2', 'User43', 'user43@example.com', 7, 83.64, datetime.date(2024, 7, 28), {'theme': 'dark', 'notifications': True}, 'Notes for User43'],
                ['44', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer2', 'User44', 'user44@example.com', 5, 74.0, datetime.date(2024, 12, 28), {'theme': 'light', 'notifications': False}, 'Notes for User44'],
                ['45', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer3', 'User45', 'user45@example.com', 3, 34.66, datetime.date(2024, 2, 4), {'theme': 'light', 'notifications': False}, 'Notes for User45'],
                ['46', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-04T00:00:00+00:00'), 'Customer3', 'User46', 'user46@example.com', 6, 13.51, datetime.date(2024, 7, 13), {'theme': 'light', 'notifications': False}, 'Notes for User46'],
                ['47', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer1', 'User47', 'user47@example.com', 8, 39.92, datetime.date(2024, 1, 24), {'theme': 'dark', 'notifications': False}, 'Notes for User47'],
                ['48', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-04T00:00:00+00:00'), 'Customer1', 'User48', 'user48@example.com', 2, 73.54, datetime.date(2024, 4, 8), {'theme': 'dark', 'notifications': False}, 'Notes for User48'],
                ['49', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer1', 'User49', 'user49@example.com', 5, 11.1, datetime.date(2024, 11, 26), {'theme': 'light', 'notifications': False}, 'Notes for User49'],
                ['50', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer3', 'User50', 'user50@example.com', 10, 44.79, datetime.date(2024, 7, 29), {'theme': 'dark', 'notifications': True}, 'Notes for User50'],
                ['51', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer1', 'User51', 'user51@example.com', 1, 57.32, datetime.date(2024, 10, 25), {'theme': 'dark', 'notifications': True}, 'Notes for User51'],
                ['52', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-05T00:00:00+00:00'), 'Customer3', 'User52', 'user52@example.com', 10, 80.22, datetime.date(2024, 6, 14), {'theme': 'light', 'notifications': True}, 'Notes for User52'],
                ['53', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer3', 'User53', 'user53@example.com', 4, 42.95, datetime.date(2024, 2, 25), {'theme': 'dark', 'notifications': False}, 'Notes for User53'],
                ['54', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer3', 'User54', 'user54@example.com', 1, 96.79, datetime.date(2024, 7, 15), {'theme': 'light', 'notifications': True}, 'Notes for User54'],
                ['55', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer2', 'User55', 'user55@example.com', 4, 89.56, datetime.date(2024, 12, 5), {'theme': 'dark', 'notifications': False}, 'Notes for User55'],
                ['56', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer3', 'User56', 'user56@example.com', 6, 52.15, datetime.date(2024, 5, 10), {'theme': 'light', 'notifications': False}, 'Notes for User56'],
                ['57', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-04T00:00:00+00:00'), 'Customer3', 'User57', 'user57@example.com', 10, 40.7, datetime.date(2024, 5, 8), {'theme': 'dark', 'notifications': True}, 'Notes for User57'],
                ['58', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer1', 'User58', 'user58@example.com', 6, 96.51, datetime.date(2024, 6, 23), {'theme': 'light', 'notifications': True}, 'Notes for User58'],
                ['59', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer1', 'User59', 'user59@example.com', 3, 22.93, datetime.date(2024, 1, 26), {'theme': 'dark', 'notifications': True}, 'Notes for User59'],
                ['60', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer2', 'User60', 'user60@example.com', 5, 66.02, datetime.date(2024, 7, 18), {'theme': 'dark', 'notifications': False}, 'Notes for User60'],
                ['61', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer1', 'User61', 'user61@example.com', 4, 33.98, datetime.date(2024, 9, 26), {'theme': 'dark', 'notifications': False}, 'Notes for User61'],
                ['62', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer2', 'User62', 'user62@example.com', 6, 26.83, datetime.date(2024, 9, 21), {'theme': 'dark', 'notifications': False}, 'Notes for User62'],
                ['63', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer3', 'User63', 'user63@example.com', 7, 58.92, datetime.date(2024, 4, 12), {'theme': 'light', 'notifications': False}, 'Notes for User63'],
                ['64', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer2', 'User64', 'user64@example.com', 10, 36.37, datetime.date(2024, 12, 20), {'theme': 'dark', 'notifications': True}, 'Notes for User64'],
                ['65', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer3', 'User65', 'user65@example.com', 9, 59.98, datetime.date(2024, 12, 7), {'theme': 'dark', 'notifications': True}, 'Notes for User65'],
                ['66', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-05T00:00:00+00:00'), 'Customer3', 'User66', 'user66@example.com', 3, 43.79, datetime.date(2024, 10, 19), {'theme': 'light', 'notifications': True}, 'Notes for User66'],
                ['67', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-04T00:00:00+00:00'), 'Customer1', 'User67', 'user67@example.com', 8, 29.1, datetime.date(2024, 2, 24), {'theme': 'dark', 'notifications': True}, 'Notes for User67'],
                ['68', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer3', 'User68', 'user68@example.com', 3, 91.13, datetime.date(2024, 10, 12), {'theme': 'light', 'notifications': True}, 'Notes for User68'],
                ['69', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer1', 'User69', 'user69@example.com', 2, 87.22, datetime.date(2024, 3, 4), {'theme': 'dark', 'notifications': True}, 'Notes for User69'],
                ['70', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer2', 'User70', 'user70@example.com', 3, 69.95, datetime.date(2024, 6, 7), {'theme': 'dark', 'notifications': True}, 'Notes for User70'],
                ['71', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-04T00:00:00+00:00'), 'Customer2', 'User71', 'user71@example.com', 2, 41.44, datetime.date(2024, 5, 12), {'theme': 'dark', 'notifications': True}, 'Notes for User71'],
                ['72', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-05T00:00:00+00:00'), 'Customer3', 'User72', 'user72@example.com', 1, 91.61, datetime.date(2024, 6, 4), {'theme': 'light', 'notifications': True}, 'Notes for User72'],
                ['73', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer3', 'User73', 'user73@example.com', 2, 15.23, datetime.date(2024, 2, 14), {'theme': 'light', 'notifications': False}, 'Notes for User73'],
                ['74', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-05T00:00:00+00:00'), 'Customer2', 'User74', 'user74@example.com', 8, 96.08, datetime.date(2024, 4, 9), {'theme': 'light', 'notifications': True}, 'Notes for User74'],
                ['75', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer2', 'User75', 'user75@example.com', 4, 76.82, datetime.date(2024, 9, 17), {'theme': 'dark', 'notifications': True}, 'Notes for User75'],
                ['76', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-05T00:00:00+00:00'), 'Customer3', 'User76', 'user76@example.com', 2, 85.73, datetime.date(2024, 12, 28), {'theme': 'light', 'notifications': True}, 'Notes for User76'],
                ['77', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-05T00:00:00+00:00'), 'Customer3', 'User77', 'user77@example.com', 4, 42.4, datetime.date(2024, 3, 10), {'theme': 'dark', 'notifications': True}, 'Notes for User77'],
                ['78', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer3', 'User78', 'user78@example.com', 8, 65.35, datetime.date(2024, 4, 23), {'theme': 'dark', 'notifications': True}, 'Notes for User78'],
                ['79', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-04T00:00:00+00:00'), 'Customer1', 'User79', 'user79@example.com', 5, 81.81, datetime.date(2024, 11, 8), {'theme': 'dark', 'notifications': False}, 'Notes for User79'],
                ['80', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer2', 'User80', 'user80@example.com', 4, 92.34, datetime.date(2024, 9, 3), {'theme': 'dark', 'notifications': True}, 'Notes for User80'],
                ['81', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer2', 'User81', 'user81@example.com', 3, 52.4, datetime.date(2024, 1, 28), {'theme': 'dark', 'notifications': False}, 'Notes for User81'],
                ['82', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-04T00:00:00+00:00'), 'Customer2', 'User82', 'user82@example.com', 1, 16.41, datetime.date(2024, 4, 15), {'theme': 'light', 'notifications': True}, 'Notes for User82'],
                ['83', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer1', 'User83', 'user83@example.com', 6, 70.29, datetime.date(2024, 8, 2), {'theme': 'light', 'notifications': False}, 'Notes for User83'],
                ['84', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-02T00:00:00+00:00'), 'Customer1', 'User84', 'user84@example.com', 6, 39.54, datetime.date(2024, 12, 12), {'theme': 'light', 'notifications': True}, 'Notes for User84'],
                ['85', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer1', 'User85', 'user85@example.com', 1, 55.65, datetime.date(2024, 10, 18), {'theme': 'light', 'notifications': False}, 'Notes for User85'],
                ['86', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer3', 'User86', 'user86@example.com', 4, 77.6, datetime.date(2024, 9, 24), {'theme': 'light', 'notifications': False}, 'Notes for User86'],
                ['87', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer3', 'User87', 'user87@example.com', 1, 81.69, datetime.date(2024, 5, 12), {'theme': 'dark', 'notifications': True}, 'Notes for User87'],
                ['88', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer2', 'User88', 'user88@example.com', 1, 34.37, datetime.date(2024, 4, 6), {'theme': 'light', 'notifications': True}, 'Notes for User88'],
                ['89', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer3', 'User89', 'user89@example.com', 3, 31.57, datetime.date(2024, 12, 27), {'theme': 'dark', 'notifications': True}, 'Notes for User89'],
                ['90', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer2', 'User90', 'user90@example.com', 5, 73.71, datetime.date(2024, 6, 1), {'theme': 'light', 'notifications': False}, 'Notes for User90'],
                ['91', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer1', 'User91', 'user91@example.com', 9, 12.74, datetime.date(2024, 12, 17), {'theme': 'light', 'notifications': False}, 'Notes for User91'],
                ['92', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer2', 'User92', 'user92@example.com', 6, 90.76, datetime.date(2024, 1, 2), {'theme': 'light', 'notifications': False}, 'Notes for User92'],
                ['93', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer1', 'User93', 'user93@example.com', 8, 49.29, datetime.date(2024, 9, 13), {'theme': 'light', 'notifications': False}, 'Notes for User93'],
                ['94', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer3', 'User94', 'user94@example.com', 8, 22.58, datetime.date(2024, 6, 20), {'theme': 'dark', 'notifications': False}, 'Notes for User94'],
                ['95', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-04T00:00:00+00:00'), 'Customer2', 'User95', 'user95@example.com', 3, 81.34, datetime.date(2024, 4, 12), {'theme': 'light', 'notifications': False}, 'Notes for User95'],
                ['96', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-03T00:00:00+00:00'), 'Customer3', 'User96', 'user96@example.com', 10, 47.45, datetime.date(2024, 1, 1), {'theme': 'dark', 'notifications': False}, 'Notes for User96'],
                ['97', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-05T00:00:00+00:00'), 'Customer1', 'User97', 'user97@example.com', 8, 85.96, datetime.date(2024, 10, 31), {'theme': 'light', 'notifications': True}, 'Notes for User97'],
                ['98', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), 'Customer3', 'User98', 'user98@example.com', 6, 68.55, datetime.date(2024, 8, 16), {'theme': 'light', 'notifications': True}, 'Notes for User98'],
                ['99', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-05T00:00:00+00:00'), 'Customer2', 'User99', 'user99@example.com', 9, 39.18, datetime.date(2024, 1, 29), {'theme': 'light', 'notifications': False}, 'Notes for User99'],
                ['100', datetime.datetime.fromisoformat('2025-01-01T00:00:00+00:00'), datetime.datetime.fromisoformat('2025-01-04T00:00:00+00:00'), 'Customer1', 'User100', 'user100@example.com', 9, 35.7, datetime.date(2024, 8, 1), {'theme': 'light', 'notifications': False}, 'Notes for User100']
            
        ]

        # Filter to a specific customer ID
        if self._config.get('streams'):
            customer_id = self._config.get('streams')[0].get('filters', {}).get('customer_id', None)
            batch = [r for r in batch \
                if r[3] == customer_id]

        # Filter to a specific date range
        if self._mode.type == Mode.INCREMENTAL:
            batch = [r for r in batch \
                if r[2] >= self._mode.start and r[2] < self._mode.end]

        batch = [stream.to_record(r) for r in batch]

        progress = Progress(
            f"source+memory://{self._namespace}/{stream.schema_name}/{stream.name}",
            total=len(batch),
            processed=0
        )
        if callable(progress_callback):
            progress.subscribe(progress_callback)

        self._cache.write(stream, batch)

        progress.update(len(batch))

        return Dataset(
            self._namespace, 
            self._streams, 
            self._cache,
            meta={'batch_id': self._batch_id, 'dt': self._dt}
        )