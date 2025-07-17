import pytest
import hashlib
import pyarrow as pa
from datetime import datetime
from pontoon import Stream, Record 


class TestStream:

    schema = pa.schema([('id', pa.int64()), ('name', pa.string()), ('age', pa.int64())])

    def test__to_record(self):
        stream = Stream('users', 'pontoon', self.schema)
        data = [0, 'Mike', 35]
        record = stream.to_record(data)
        assert isinstance(record, Record)
        assert isinstance(record.data, list)
        assert record.data[0] == 0
        assert record.data[1] == 'Mike'
        assert record.data[2] == 35

    def test__with_field(self):
        stream = Stream('users', 'pontoon', self.schema)
        assert stream.schema.equals(self.schema)

        stream.with_field('job', pa.string(), 'engineer')
        self.schema = self.schema.append(pa.field('job', pa.string()))
        assert stream.schema.equals(self.schema)

        data = [0, 'Mike', 35]
        record = stream.to_record(data)
        assert record.data == [0, 'Mike', 35, 'engineer']

    
    def test__drop_field(self):
        stream = Stream('users', 'pontoon', self.schema)
        assert stream.schema.equals(self.schema)

        stream.drop_field('name')
        self.schema = self.schema.remove(1)
        assert stream.schema.equals(self.schema)

        data = [0, 'Mike', 35]
        record = stream.to_record(data)
        assert record.data == [0, 35]
    

    def test__with_extra_fields(self):
        now = datetime(2025, 1, 1)
        now_str = now.isoformat()

        stream = Stream('users', 'pontoon', self.schema)
        stream.with_checksum()
        stream.with_batch_id('batch1')
        stream.with_version('1.0.0')
        stream.with_last_synced_at(now)

        self.schema = self.schema.append(pa.field('pontoon__checksum', pa.string()))
        self.schema = self.schema.append(pa.field('pontoon__batch_id', pa.string()))
        self.schema = self.schema.append(pa.field('pontoon__version', pa.string()))
        self.schema = self.schema.append(pa.field('pontoon__last_synced_at', pa.timestamp('us', tz='UTC')))

        assert stream.schema.equals(self.schema)

        data = [0, 'Mike', 35]
        checksum = hashlib.md5('0Mike35'.encode('utf-8')).hexdigest()
        record = stream.to_record(data)
        assert record.data == [0, 'Mike', 35, checksum, 'batch1', '1.0.0', now_str]

        stream.drop_field('name')
        record = stream.to_record(data)
        assert record.data == [0, 35, checksum, 'batch1', '1.0.0', now_str]



    def test__infer_schema(self):
        cols = ['id', 'name', 'age']
        sample = [0, 'Mike', 35]
        inferred_schema  = Stream.infer_schema(cols, sample)
        assert inferred_schema.equals(self.schema)
    
    def test__build_schema(self):
        cols = [('id', int), ('name', str), ('age', int)]
        built_schema = Stream.build_schema(cols)
        assert built_schema.equals(self.schema)
    
    def test__stream_properties(self):
        stream = Stream('users', 'pontoon', self.schema)
        assert stream.schema_name == 'pontoon'
        assert stream.name == 'users'
        assert stream.schema.equals(self.schema)
