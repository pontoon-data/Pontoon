from sqlalchemy import text
from pontoon.base import Integrity, Stream, Dataset, Destination


class MockIntegrity(Integrity):
    """ Integrity checker for mock and test harness destinations """
    
    def __init__(self):
        pass
    
    def check_batch_volume(self, ds:Dataset):
        return True


class SQLIntegrity(Integrity):
    """ Integrity checker for SQL based destinations """
    
    def __init__(self, engine, prefix='pontoon__'):
        self._engine = engine
        self._prefix = prefix

    def check_batch_volume(self, ds:Dataset):
        with self._engine.connect() as conn:
            for stream in ds.streams:
                result = conn.execute(text(f"SELECT COUNT(1) FROM {stream.schema_name}.{stream.name} WHERE {self._prefix}batch_id='{ds.meta.get('batch_id')}'"))
                batch_count = result.scalar_one()
                stream_size = ds.size(stream)
                if stream_size != batch_count:
                    raise Exception(
                        f"Integrity check failed for {stream.schema_name}.{stream.name}: "
                        f"loaded={batch_count}, expected={stream_size}"
                    )

class S3Integrity(Integrity):
    """ Integrity checker for S3 destinations """

    def __init__(self, client):
        self._client = client
        raise NotImplementedError("S3Integrity checker is not implemented yet")

    def check_batch_volume(self, ds:Dataset):
        return True


class GCSIntegrity(Integrity):
    """ Integrity checker for GCS based destinations """

    def __init__(self, client):
        self._client = client
        raise NotImplementedError("GCSIntegrity checker is not implemented yet")

    def check_batch_volume(self, ds:Dataset):
        return True


class SMSIntegrity(Integrity):
    """ Integrity checker for SMS (Snowflake) based destinations """

    def __init__(self, client):
        self._client = client
        raise NotImplementedError("SMSIntegrity checker is not implemented yet")

    def check_batch_volume(self, ds:Dataset):
        return True