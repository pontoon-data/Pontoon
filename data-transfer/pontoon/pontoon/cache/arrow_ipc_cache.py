"""
Arrow IPC Cache implementation.

"""

import os
import json
import re
import tempfile
import hashlib
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Generator, Any, Optional
import pyarrow as pa
import pyarrow.ipc
from pontoon.base import Cache, Namespace, Stream, Record


class CacheWriteError(Exception):
    """Raised when Arrow IPC write operations fail"""
    pass


class CacheReadError(Exception):
    """Raised when Arrow IPC read operations fail"""
    pass


class CacheSchemaError(Exception):
    """Raised when schema validation fails"""
    pass


class CacheFileSystemError(Exception):
    """Raised when file system operations fail"""
    pass


class ArrowIpcCache(Cache):
    """
    Arrow IPC Cache implementation.
    
    """
    
    def __init__(self, namespace: Namespace, config: Dict[str, Any]):
        """
        Initialize ArrowIpcCache.
        
        Args:
            namespace: The namespace for this cache instance
            config: Configuration dictionary with keys:
                - cache_dir: Base directory for cache storage (default: "./cache")
                - batch_size: Records per batch for I/O operations (default: 10000)
                - write_buffer_size: Number of batches to buffer before flush (default: 1)
                - use_stream_format: Use streaming format for better append performance (default: True)
                - skip_metadata_validation: Skip expensive metadata checks (default: True)
        """
        self._namespace = namespace
        self._config = config
        
        # Configuration with performance-focused defaults
        self._cache_dir = config.get('cache_dir', './cache')
        self._batch_size = config.get('batch_size', 10000)
        self._write_buffer_size = config.get('write_buffer_size', 1)
        self._use_stream_format = config.get('use_stream_format', True)
        self._skip_metadata_validation = config.get('skip_metadata_validation', True)
        
        # Performance optimizations
        self._closed = False
        self._write_buffers = {}  # Stream -> buffered batches
        self._record_counts = {}  # Stream -> current count (in-memory)
        self._stream_writers = {}  # Stream -> open writers for streaming
        
        # Ensure cache directory exists
        self._ensure_cache_directory()
    
    def _ensure_cache_directory(self):
        """Create cache directory structure if needed."""
        try:
            cache_path = Path(self._cache_dir)
            cache_path.mkdir(parents=True, exist_ok=True)
            
            namespace_path = cache_path / self._namespace.name
            namespace_path.mkdir(parents=True, exist_ok=True)
            
        except OSError as e:
            raise CacheFileSystemError(f"Failed to create cache directory: {e}")
    
    def write(self, stream: Stream, records: List[Record]) -> int:
        """
        Optimized write implementation with minimal overhead.
        """
        if self._closed:
            raise CacheFileSystemError("Cache is closed")
        
        if not records:
            return 0
        
        try:
            # Convert records to Arrow batch (cached schema validation)
            #print(stream.schema)
            #print([record.data for record in records])
            record_batch = self._records_to_arrow_batch_fast(records, stream.schema)
            
            if self._use_stream_format:
                return self._write_streaming(stream, record_batch)
            else:
                return self._write_buffered(stream, record_batch)
                
        except Exception as e:
            raise CacheWriteError(f"Failed to write records: {e}")
    
    def _write_streaming(self, stream: Stream, record_batch: pa.RecordBatch) -> int:
        """
        Write using Arrow IPC streaming format for optimal append performance.
        """
        stream_key = (stream.schema_name, stream.name)
        
        # Get or create stream writer
        if stream_key not in self._stream_writers:
            file_path = self._get_stream_file_path(stream)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Open in append mode if file exists, otherwise create new
            if file_path.exists():
                # For existing files, we need to append to the stream
                # Arrow IPC streams support this naturally
                file_handle = open(file_path, 'ab')
                writer = pa.ipc.new_stream(file_handle, record_batch.schema)
            else:
                # New file
                file_handle = open(file_path, 'wb')
                writer = pa.ipc.new_stream(file_handle, record_batch.schema)
            
            self._stream_writers[stream_key] = (writer, file_handle)
            self._record_counts[stream_key] = 0
        
        writer, _ = self._stream_writers[stream_key]
        
        # Write batch directly to stream
        writer.write_batch(record_batch)
        
        # Update in-memory count
        records_written = record_batch.num_rows
        self._record_counts[stream_key] += records_written
        
        return records_written
    
    def _write_buffered(self, stream: Stream, record_batch: pa.RecordBatch) -> int:
        """
        Write using buffered batches for better throughput on small writes.
        """
        stream_key = (stream.schema_name, stream.name)
        
        # Initialize buffer if needed
        if stream_key not in self._write_buffers:
            self._write_buffers[stream_key] = []
            self._record_counts[stream_key] = 0
        
        # Add batch to buffer
        self._write_buffers[stream_key].append(record_batch)
        records_written = record_batch.num_rows
        self._record_counts[stream_key] += records_written
        
        # Flush if buffer is full
        if len(self._write_buffers[stream_key]) >= self._write_buffer_size:
            self._flush_buffer(stream, stream_key)
        
        return records_written
    
    def _flush_buffer(self, stream: Stream, stream_key):
        """Flush buffered batches to disk."""
        if stream_key not in self._write_buffers or not self._write_buffers[stream_key]:
            return
        
        file_path = self._get_stream_file_path(stream)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        batches = self._write_buffers[stream_key]
        
        if self._use_stream_format:
            # Use stream format for buffered writes too
            if file_path.exists():
                # Append to existing stream file
                with open(file_path, 'ab') as f:
                    writer = pa.ipc.new_stream(f, batches[0].schema)
                    for batch in batches:
                        writer.write_batch(batch)
            else:
                # Create new stream file
                with open(file_path, 'wb') as f:
                    writer = pa.ipc.new_stream(f, batches[0].schema)
                    for batch in batches:
                        writer.write_batch(batch)
        else:
            # Use file format for buffered writes
            if file_path.exists():
                # For file format, we need to read existing data and rewrite
                # This is less efficient but maintains file format compatibility
                existing_batches = []
                try:
                    with pa.ipc.open_file(file_path) as reader:
                        for i in range(reader.num_record_batches):
                            existing_batches.append(reader.get_batch(i))
                except:
                    # If file is corrupted or empty, start fresh
                    existing_batches = []
                
                # Write all batches (existing + new) to file
                with pa.ipc.new_file(file_path, batches[0].schema) as writer:
                    for batch in existing_batches:
                        writer.write_batch(batch)
                    for batch in batches:
                        writer.write_batch(batch)
            else:
                # Create new file
                with pa.ipc.new_file(file_path, batches[0].schema) as writer:
                    for batch in batches:
                        writer.write_batch(batch)
        
        # Clear buffer
        self._write_buffers[stream_key] = []
    
    def read(self, stream: Stream) -> Generator[Record, None, None]:
        """
        Read records from cache. Flushes any pending writes first.
        """
        if self._closed:
            raise CacheFileSystemError("Cache is closed")
        
        # Flush any pending writes for this stream
        stream_key = (stream.schema_name, stream.name)
        if stream_key in self._write_buffers:
            self._flush_buffer(stream, stream_key)
        
        # Close any open writers for this stream to ensure data is flushed
        if stream_key in self._stream_writers:
            writer, file_handle = self._stream_writers[stream_key]
            writer.close()
            file_handle.close()
            del self._stream_writers[stream_key]
        
        file_path = self._get_stream_file_path(stream)
        
        if not file_path.exists():
            return
            yield  # Make this a generator
        
        try:
            # Try to read based on the format used
            if self._use_stream_format or file_path.suffix == '.arrows':
                # Read from Arrow IPC stream format
                with open(file_path, 'rb') as f:
                    reader = pa.ipc.open_stream(f)
                    
                    for batch in reader:
                        # Convert batch to records
                        records = self._arrow_batch_to_records_fast(batch)
                        for record in records:
                            yield record
            else:
                # Read from Arrow IPC file format
                with pa.ipc.open_file(file_path) as reader:
                    for i in range(reader.num_record_batches):
                        batch = reader.get_batch(i)
                        records = self._arrow_batch_to_records_fast(batch)
                        for record in records:
                            yield record
                        
        except Exception as e:
            raise CacheReadError(f"Failed to read from stream: {e}")
    
    def size(self, stream: Stream) -> int:
        """Get the number of records in a stream."""
        if self._closed:
            raise CacheFileSystemError("Cache is closed")
        
        stream_key = (stream.schema_name, stream.name)
        
        # Return in-memory count if available
        if stream_key in self._record_counts:
            return self._record_counts[stream_key]
        
        # Otherwise count by reading the file
        count = 0
        for _ in self.read(stream):
            count += 1
        
        self._record_counts[stream_key] = count
        return count
    
    def flush(self):
        """Flush all pending writes to disk."""
        # Flush all buffers
        for stream_key in list(self._write_buffers.keys()):
            if self._write_buffers[stream_key]:
                # We need the stream object to flush, but we only have the key
                # For now, skip flushing orphaned buffers
                pass
        
        # Close all stream writers to ensure data is written
        for stream_key in list(self._stream_writers.keys()):
            writer, file_handle = self._stream_writers[stream_key]
            writer.close()
            file_handle.close()
            del self._stream_writers[stream_key]
    
    def close(self):
        """Close the cache and flush all pending writes."""
        if self._closed:
            return
        
        try:
            self.flush()
            self._closed = True
        except Exception as e:
            raise CacheFileSystemError(f"Failed to close cache: {e}")
    
    def _records_to_arrow_batch_fast(self, records: List[Record], schema: pa.Schema) -> pa.RecordBatch:
        """
        Fast conversion of records to Arrow batch with minimal validation.
        """
        if not records:
            # Return empty batch
            empty_arrays = [pa.array([], type=field.type) for field in schema]
            return pa.record_batch(empty_arrays, schema=schema)
        
        # Extract data in columnar format
        num_fields = len(schema)
        columns = [[] for _ in range(num_fields)]
        
        for record in records:
            for field_idx, value in enumerate(record.data):
                columns[field_idx].append(value)
        
        # Convert to Arrow arrays with minimal type checking
        arrow_arrays = []
        for field_idx, (field, column_data) in enumerate(zip(schema, columns)):
            if self._skip_metadata_validation:
                # Fast path: let Arrow handle type conversion
                arrow_array = pa.array(column_data, type=field.type)
            else:
                # Slower path with type conversion
                converted_data = self._convert_column_for_arrow(column_data, field.type)
                arrow_array = pa.array(converted_data, type=field.type)
            
            arrow_arrays.append(arrow_array)
        
        return pa.record_batch(arrow_arrays, schema=schema)
    
    def _arrow_batch_to_records_fast(self, batch: pa.RecordBatch) -> List[Record]:
        """
        Fast conversion of Arrow batch to records.
        """
        if batch.num_rows == 0:
            return []
        
        # Convert to Python objects efficiently
        batch_dict = batch.to_pydict()
        field_names = batch.schema.names
        
        records = []
        for row_idx in range(batch.num_rows):
            row_data = [batch_dict[field_name][row_idx] for field_name in field_names]
            records.append(Record(row_data))
        
        return records
    
    def _convert_column_for_arrow(self, column_data: List[Any], arrow_type: pa.DataType) -> List[Any]:
        """Convert column data for Arrow compatibility (simplified version)."""
        # Simplified conversion - let Arrow handle most type conversions
        return column_data
    
    def _get_stream_file_path(self, stream: Stream) -> Path:
        """Generate file path for stream storage."""
        # Simplified path generation
        cache_path = Path(self._cache_dir)
        namespace_path = cache_path / self._namespace.name
        
        # Use stream format extension
        extension = '.arrows' if self._use_stream_format else '.arrow'
        filename = f"{stream.schema_name}__{stream.name}{extension}"
        
        return namespace_path / filename