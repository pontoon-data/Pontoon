import pytest
from datetime import datetime, timedelta, timezone
import pyarrow as pa
from pontoon import Stream, Mode
from pontoon.source.sql_source import SQLSource, SQLUtil


class TestSQLSource:
    """ 
    Tests some of the machinery used for selecting data from SQL source dbs

    It involves generating a SELECT statement based on cursor columns and other filters.
    
    """

    def test_build_select_query(self):

        # dummy table schema
        schema = pa.schema([('id', pa.int64()), ('data', pa.string()), ('event_time', pa.timestamp('us',tz='UTC')), ('user_id', pa.int64())])
        
        # stream is our internal representation of a table, essentailly
        stream = Stream('events', 'pontoon', schema, primary_field='id', cursor_field='event_time')
        now = datetime(2025, 1, 14, 18, 49, 32, 0)
        
        # mode is a sync modality configuration - incremental
        mode = Mode({
            'type': Mode.INCREMENTAL,
            'start': now - timedelta(hours=24),
            'end': now
        })

        # generate select query 
        select_query = SQLUtil.build_select_query(stream, mode)
        assert select_query == "SELECT id,data,event_time,user_id FROM pontoon.events WHERE event_time >= '2025-01-13T18:49:32' AND event_time < '2025-01-14T18:49:32'"  

        # generate select query with additional WHERE filters 
        stream = Stream('events', 'pontoon', schema, primary_field='id', cursor_field='event_time', filters={'user_id': 1000})
        select_query = SQLUtil.build_select_query(stream, mode)
        assert select_query == "SELECT id,data,event_time,user_id FROM pontoon.events WHERE event_time >= '2025-01-13T18:49:32' AND event_time < '2025-01-14T18:49:32' AND user_id = 1000"