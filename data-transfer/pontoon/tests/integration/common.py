import os
import json
import glob
import uuid
import psutil
import pytest
from datetime import datetime, timezone

from sqlalchemy import create_engine, inspect, MetaData, Table, text

from pontoon import configure_logging
from pontoon import get_source, get_destination, get_source_by_vendor, get_destination_by_vendor
from pontoon import SqliteCache
from pontoon import Progress, Mode
from pontoon import SourceConnectionFailed, SourceStreamDoesNotExist, SourceStreamInvalidSchema
from pontoon import DestinationConnectionFailed, DestinationStreamInvalidSchema

from dotenv import load_dotenv
load_dotenv()

def clear_cache_files():
    for f in glob.glob("*_cache.db"):
        os.remove(f)


def read_progress_handler(progress:Progress):
    print(f"{progress.entity()}: {progress.processed}/{progress.total} records ({progress.percent}%)")


def write_progress_handler(progress:Progress):
    print(f"{progress.entity()}: {progress.processed}/{progress.total} records ({progress.percent}%)")


def get_memory_source(mode_config={}, streams_config={}):
    return get_source(
        get_source_by_vendor('memory'),
        config={
            'with': {
                'batch_id': True,
                'last_sync': True
            },
            'mode': Mode({
                'type': Mode.INCREMENTAL,
                'period': Mode.DAILY,
                'start': datetime(2025, 1, 1, tzinfo=timezone.utc),
                'end': datetime(2025, 1, 2, tzinfo=timezone.utc)
            } | mode_config),
            'streams': [{
                'filters': {'customer_id': 'Customer1'}
            } | streams_config]
        },
        cache_implementation = SqliteCache,
        cache_config = {
            'db': f"_memory_{uuid.uuid4()}_cache.db",
            'chunk_size': 1024
        }
    )
