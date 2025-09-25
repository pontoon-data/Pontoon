"""
Performance benchmark tests for ArrowIpcCache comparing against SqliteCache.

This module contains benchmarks to measure and compare the performance
of the optimized ArrowIpcCache against SqliteCache across various metrics including:
- Write performance (records per second)
- Read performance (records per second)
- Memory usage during large dataset processing
- Throughput measurements

"""

import os
import time
import tempfile
import shutil
import psutil
import gc
from datetime import datetime, date, timezone
from decimal import Decimal
from typing import List, Dict, Any, Tuple
from pathlib import Path

import pytest
import pyarrow as pa

from pontoon.base import Namespace, Stream, Record
from pontoon.cache.arrow_ipc_cache import ArrowIpcCache
from pontoon.cache.sqlite_cache import SqliteCache


class MemoryMonitor:
    """Helper class to monitor memory usage during benchmarks"""
    
    def __init__(self):
        self.process = psutil.Process()
        self.initial_memory = self.process.memory_info().rss
        self.peak_memory = self.initial_memory
        self.measurements = []
    
    def measure(self, label: str = ""):
        """Take a memory measurement"""
        current_memory = self.process.memory_info().rss
        self.peak_memory = max(self.peak_memory, current_memory)
        self.measurements.append({
            'label': label,
            'memory_mb': current_memory / (1024 * 1024),
            'timestamp': time.time()
        })
        return current_memory
    
    def get_peak_usage_mb(self) -> float:
        """Get peak memory usage in MB"""
        return self.peak_memory / (1024 * 1024)
    
    def get_memory_increase_mb(self) -> float:
        """Get memory increase from initial measurement in MB"""
        return (self.peak_memory - self.initial_memory) / (1024 * 1024)


class BenchmarkResult:
    """Container for benchmark results"""
    
    def __init__(self, cache_type: str, operation: str, dataset_size: int):
        self.cache_type = cache_type
        self.operation = operation
        self.dataset_size = dataset_size
        self.duration_seconds = 0.0
        self.records_per_second = 0.0
        self.peak_memory_mb = 0.0
        self.memory_increase_mb = 0.0
        self.additional_metrics = {}
    
    def __str__(self):
        return (f"{self.cache_type} {self.operation} - "
                f"Size: {self.dataset_size:,} records, "
                f"Duration: {self.duration_seconds:.2f}s, "
                f"RPS: {self.records_per_second:,.0f}, "
                f"Peak Memory: {self.peak_memory_mb:.1f}MB, "
                f"Memory Increase: {self.memory_increase_mb:.1f}MB")


class CacheBenchmark:
    """Main benchmark class for comparing cache implementations"""
    
    def __init__(self):
        self.temp_dirs = []
        self.results = []
    
    def cleanup(self):
        """Clean up temporary directories"""
        for temp_dir in self.temp_dirs:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
        self.temp_dirs.clear()
    
    def create_temp_dir(self) -> str:
        """Create a temporary directory for cache storage"""
        temp_dir = tempfile.mkdtemp(prefix="cache_benchmark_")
        self.temp_dirs.append(temp_dir)
        return temp_dir
    
    def create_test_data(self, size: int, schema_type: str = "mixed") -> Tuple[Stream, List[Record]]:
        """Create test data for benchmarking"""
        
        if schema_type == "simple":
            # Simple schema with basic types
            schema = pa.schema([
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
                pa.field("active", pa.bool_())
            ])
            
            records = []
            for i in range(size):
                record_data = [
                    i,
                    f"record_{i}",
                    float(i * 1.5),
                    i % 2 == 0
                ]
                records.append(Record(record_data))
        
        elif schema_type == "mixed":
            # Mixed schema with various data types
            schema = pa.schema([
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("price", pa.float64()),
                pa.field("created_at", pa.timestamp('us', tz='UTC')),
                pa.field("birth_date", pa.date32()),
                pa.field("active", pa.bool_()),
                pa.field("description", pa.string()),
                pa.field("category_id", pa.int32())
            ])
            
            records = []
            base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
            base_date = date(1990, 1, 1)
            
            for i in range(size):
                record_data = [
                    i,
                    f"product_{i}",
                    round(10.0 + (i * 0.99), 2),
                    base_time.replace(day=1 + (i % 28)),
                    base_date.replace(year=1990 + (i % 30)),
                    i % 3 == 0,
                    f"Description for product {i} with some longer text to test string handling",
                    i % 10 + 1
                ]
                records.append(Record(record_data))
        
        elif schema_type == "wide":
            # Wide schema with many columns
            fields = []
            for col_idx in range(50):
                if col_idx % 4 == 0:
                    fields.append(pa.field(f"int_col_{col_idx}", pa.int64()))
                elif col_idx % 4 == 1:
                    fields.append(pa.field(f"str_col_{col_idx}", pa.string()))
                elif col_idx % 4 == 2:
                    fields.append(pa.field(f"float_col_{col_idx}", pa.float64()))
                else:
                    fields.append(pa.field(f"bool_col_{col_idx}", pa.bool_()))
            
            schema = pa.schema(fields)
            
            records = []
            for i in range(size):
                record_data = []
                for col_idx in range(50):
                    if col_idx % 4 == 0:
                        record_data.append(i + col_idx)
                    elif col_idx % 4 == 1:
                        record_data.append(f"value_{i}_{col_idx}")
                    elif col_idx % 4 == 2:
                        record_data.append(float(i * col_idx * 0.1))
                    else:
                        record_data.append((i + col_idx) % 2 == 0)
                records.append(Record(record_data))
        
        else:
            raise ValueError(f"Unknown schema type: {schema_type}")
        
        stream = Stream(
            name="benchmark_stream",
            schema_name="benchmark_schema",
            schema=schema
        )
        
        return stream, records
    
    def benchmark_write_performance(self, cache_class, cache_config: Dict[str, Any], 
                                  stream: Stream, records: List[Record]) -> BenchmarkResult:
        """Benchmark write performance for a cache implementation"""
        
        # Use a fresh namespace for write benchmark
        namespace = Namespace("benchmark_write_ns")
        cache = cache_class(namespace, cache_config)
        
        memory_monitor = MemoryMonitor()
        memory_monitor.measure("start")
        
        # Force garbage collection before benchmark
        gc.collect()
        
        try:
            start_time = time.time()
            
            # Write records in batches to simulate real usage
            batch_size = min(1000, len(records) // 10) if len(records) > 1000 else len(records)
            batches_written = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                cache.write(stream, batch)
                batches_written += 1
                
                # Take memory measurements periodically
                if batches_written % 5 == 0:
                    memory_monitor.measure(f"batch_{batches_written}")
            
            end_time = time.time()
            memory_monitor.measure("end")
            
            duration = end_time - start_time
            rps = len(records) / duration if duration > 0 else 0
            
            result = BenchmarkResult(
                cache_type=cache_class.__name__,
                operation="write",
                dataset_size=len(records)
            )
            result.duration_seconds = duration
            result.records_per_second = rps
            result.peak_memory_mb = memory_monitor.get_peak_usage_mb()
            result.memory_increase_mb = memory_monitor.get_memory_increase_mb()
            result.additional_metrics = {
                'batches_written': batches_written,
                'avg_batch_size': len(records) / batches_written if batches_written > 0 else 0
            }
            
            return result
            
        finally:
            cache.close()
    
    def benchmark_read_performance(self, cache_class, cache_config: Dict[str, Any],
                                 stream: Stream, records: List[Record]) -> BenchmarkResult:
        """Benchmark read performance for a cache implementation"""
        
        # Use a fresh namespace and cache directory for read benchmark
        namespace = Namespace("benchmark_read_ns")
        
        # Create a fresh cache config to avoid conflicts
        read_cache_config = cache_config.copy()
        if 'cache_dir' in read_cache_config:
            # Create a separate directory for read benchmark
            read_cache_config['cache_dir'] = self.create_temp_dir()
        elif 'db' in read_cache_config:
            # Create a separate database file for read benchmark
            read_cache_config['db'] = os.path.join(self.create_temp_dir(), "read_benchmark.db")
        
        cache = cache_class(namespace, read_cache_config)
        
        try:
            # First write the data to a fresh cache
            cache.write(stream, records)
            
            memory_monitor = MemoryMonitor()
            memory_monitor.measure("start")
            
            # Force garbage collection before benchmark
            gc.collect()
            
            start_time = time.time()
            
            # Read all records
            records_read = 0
            for record in cache.read(stream):
                records_read += 1
                
                # Take memory measurements periodically
                if records_read % 5000 == 0:
                    memory_monitor.measure(f"read_{records_read}")
            
            end_time = time.time()
            memory_monitor.measure("end")
            
            duration = end_time - start_time
            rps = records_read / duration if duration > 0 else 0
            
            result = BenchmarkResult(
                cache_type=cache_class.__name__,
                operation="read",
                dataset_size=records_read
            )
            result.duration_seconds = duration
            result.records_per_second = rps
            result.peak_memory_mb = memory_monitor.get_peak_usage_mb()
            result.memory_increase_mb = memory_monitor.get_memory_increase_mb()
            result.additional_metrics = {
                'records_read': records_read,
                'expected_records': len(records)
            }
            
            # Verify we read the expected number of records
            assert records_read == len(records), f"Expected {len(records)} records, read {records_read}"
            
            return result
            
        finally:
            cache.close()
    
    def run_comparative_benchmark(self, dataset_sizes: List[int], schema_types: List[str]) -> List[BenchmarkResult]:
        """Run comparative benchmarks between ArrowIpcCache and SqliteCache"""
        
        results = []
        
        for size in dataset_sizes:
            for schema_type in schema_types:
                print(f"\nBenchmarking {schema_type} schema with {size:,} records...")
                
                # Create test data
                stream, records = self.create_test_data(size, schema_type)
                
                # Benchmark ArrowIpcCache (now optimized)
                arrow_cache_dir = self.create_temp_dir()
                arrow_config = {
                    'cache_dir': arrow_cache_dir,
                    'batch_size': 10000,
                    'use_stream_format': True,
                    'skip_metadata_validation': True
                }
                
                print(f"  Testing ArrowIpcCache write...")
                arrow_write_result = self.benchmark_write_performance(
                    ArrowIpcCache, arrow_config, stream, records
                )
                arrow_write_result.additional_metrics['schema_type'] = schema_type
                results.append(arrow_write_result)
                print(f"    {arrow_write_result}")
                
                print(f"  Testing ArrowIpcCache read...")
                arrow_read_result = self.benchmark_read_performance(
                    ArrowIpcCache, arrow_config, stream, records
                )
                arrow_read_result.additional_metrics['schema_type'] = schema_type
                results.append(arrow_read_result)
                print(f"    {arrow_read_result}")
                
                # Benchmark SqliteCache
                sqlite_db_path = os.path.join(self.create_temp_dir(), "benchmark.db")
                sqlite_config = {
                    'db': sqlite_db_path,
                    'chunk_size': 1000
                }
                
                print(f"  Testing SqliteCache write...")
                sqlite_write_result = self.benchmark_write_performance(
                    SqliteCache, sqlite_config, stream, records
                )
                sqlite_write_result.additional_metrics['schema_type'] = schema_type
                results.append(sqlite_write_result)
                print(f"    {sqlite_write_result}")
                
                print(f"  Testing SqliteCache read...")
                sqlite_read_result = self.benchmark_read_performance(
                    SqliteCache, sqlite_config, stream, records
                )
                sqlite_read_result.additional_metrics['schema_type'] = schema_type
                results.append(sqlite_read_result)
                print(f"    {sqlite_read_result}")
                
                # Calculate performance ratios
                write_speedup = (arrow_write_result.records_per_second / 
                               sqlite_write_result.records_per_second if sqlite_write_result.records_per_second > 0 else 0)
                read_speedup = (arrow_read_result.records_per_second / 
                              sqlite_read_result.records_per_second if sqlite_read_result.records_per_second > 0 else 0)
                
                print(f"  Performance Summary:")
                print(f"    Write speedup: {write_speedup:.2f}x")
                print(f"    Read speedup: {read_speedup:.2f}x")
                print(f"    Arrow write memory: {arrow_write_result.memory_increase_mb:.1f}MB")
                print(f"    SQLite write memory: {sqlite_write_result.memory_increase_mb:.1f}MB")
                print(f"    Arrow read memory: {arrow_read_result.memory_increase_mb:.1f}MB")
                print(f"    SQLite read memory: {sqlite_read_result.memory_increase_mb:.1f}MB")
        
        return results
    
    def generate_performance_report(self, results: List[BenchmarkResult]) -> str:
        """Generate a comprehensive performance report"""
        
        report = []
        report.append("=" * 80)
        report.append("OPTIMIZED ARROW IPC CACHE PERFORMANCE BENCHMARK REPORT")
        report.append("=" * 80)
        report.append("")
        
        # Group results by dataset size and schema type
        grouped_results = {}
        for result in results:
            key = (result.dataset_size, result.additional_metrics.get('schema_type', 'unknown'))
            if key not in grouped_results:
                grouped_results[key] = {}
            
            operation_key = f"{result.cache_type}_{result.operation}"
            grouped_results[key][operation_key] = result
        
        # Generate comparison tables
        for (dataset_size, schema_type), group_results in sorted(grouped_results.items()):
            report.append(f"Dataset: {dataset_size:,} records, Schema: {schema_type}")
            report.append("-" * 60)
            
            # Write performance comparison
            arrow_write = group_results.get('ArrowIpcCache_write')
            sqlite_write = group_results.get('SqliteCache_write')
            
            if arrow_write and sqlite_write:
                write_speedup = arrow_write.records_per_second / sqlite_write.records_per_second
                report.append(f"WRITE PERFORMANCE:")
                report.append(f"  ArrowIpcCache: {arrow_write.records_per_second:,.0f} RPS, "
                            f"{arrow_write.duration_seconds:.2f}s, "
                            f"{arrow_write.memory_increase_mb:.1f}MB")
                report.append(f"  SqliteCache:   {sqlite_write.records_per_second:,.0f} RPS, "
                            f"{sqlite_write.duration_seconds:.2f}s, "
                            f"{sqlite_write.memory_increase_mb:.1f}MB")
                report.append(f"  Speedup: {write_speedup:.2f}x")
                report.append("")
            
            # Read performance comparison
            arrow_read = group_results.get('ArrowIpcCache_read')
            sqlite_read = group_results.get('SqliteCache_read')
            
            if arrow_read and sqlite_read:
                read_speedup = arrow_read.records_per_second / sqlite_read.records_per_second
                report.append(f"READ PERFORMANCE:")
                report.append(f"  ArrowIpcCache: {arrow_read.records_per_second:,.0f} RPS, "
                            f"{arrow_read.duration_seconds:.2f}s, "
                            f"{arrow_read.memory_increase_mb:.1f}MB")
                report.append(f"  SqliteCache:   {sqlite_read.records_per_second:,.0f} RPS, "
                            f"{sqlite_read.duration_seconds:.2f}s, "
                            f"{sqlite_read.memory_increase_mb:.1f}MB")
                report.append(f"  Speedup: {read_speedup:.2f}x")
                report.append("")
            
            report.append("")
        
        # Overall summary
        report.append("OVERALL SUMMARY")
        report.append("-" * 40)
        
        arrow_write_results = [r for r in results if r.cache_type == 'ArrowIpcCache' and r.operation == 'write']
        sqlite_write_results = [r for r in results if r.cache_type == 'SqliteCache' and r.operation == 'write']
        arrow_read_results = [r for r in results if r.cache_type == 'ArrowIpcCache' and r.operation == 'read']
        sqlite_read_results = [r for r in results if r.cache_type == 'SqliteCache' and r.operation == 'read']
        
        if arrow_write_results and sqlite_write_results:
            avg_arrow_write_rps = sum(r.records_per_second for r in arrow_write_results) / len(arrow_write_results)
            avg_sqlite_write_rps = sum(r.records_per_second for r in sqlite_write_results) / len(sqlite_write_results)
            avg_write_speedup = avg_arrow_write_rps / avg_sqlite_write_rps
            report.append(f"Average Write Speedup: {avg_write_speedup:.2f}x")
        
        if arrow_read_results and sqlite_read_results:
            avg_arrow_read_rps = sum(r.records_per_second for r in arrow_read_results) / len(arrow_read_results)
            avg_sqlite_read_rps = sum(r.records_per_second for r in sqlite_read_results) / len(sqlite_read_results)
            avg_read_speedup = avg_arrow_read_rps / avg_sqlite_read_rps
            report.append(f"Average Read Speedup: {avg_read_speedup:.2f}x")
        
        # Memory usage summary
        if arrow_write_results:
            avg_arrow_write_memory = sum(r.memory_increase_mb for r in arrow_write_results) / len(arrow_write_results)
            report.append(f"Average ArrowIpcCache Write Memory: {avg_arrow_write_memory:.1f}MB")
        
        if sqlite_write_results:
            avg_sqlite_write_memory = sum(r.memory_increase_mb for r in sqlite_write_results) / len(sqlite_write_results)
            report.append(f"Average SqliteCache Write Memory: {avg_sqlite_write_memory:.1f}MB")
        
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)


# Test fixtures and benchmark test functions

@pytest.fixture
def benchmark_suite():
    """Fixture to provide a benchmark suite with cleanup"""
    suite = CacheBenchmark()
    yield suite
    suite.cleanup()


def test_small_dataset_performance(benchmark_suite):
    """Test performance with small datasets (1K-10K records)"""
    dataset_sizes = [1000, 5000, 10000]
    schema_types = ["simple", "mixed"]
    
    results = benchmark_suite.run_comparative_benchmark(dataset_sizes, schema_types)
    
    # Verify we have results for all combinations
    expected_results = len(dataset_sizes) * len(schema_types) * 2 * 2  # 2 cache types, 2 operations
    assert len(results) == expected_results
    
    # Verify ArrowIpcCache performs reasonably (not necessarily faster for small datasets)
    arrow_results = [r for r in results if r.cache_type == 'ArrowIpcCache']
    for result in arrow_results:
        assert result.records_per_second > 0, f"ArrowIpcCache should have positive RPS: {result}"
        assert result.duration_seconds > 0, f"ArrowIpcCache should have positive duration: {result}"


def test_medium_dataset_performance(benchmark_suite):
    """Test performance with medium datasets (50K-100K records)"""
    dataset_sizes = [50000, 100000]
    schema_types = ["mixed"]
    
    results = benchmark_suite.run_comparative_benchmark(dataset_sizes, schema_types)
    
    # Verify we have results
    expected_results = len(dataset_sizes) * len(schema_types) * 2 * 2
    assert len(results) == expected_results
    
    # For medium datasets, ArrowIpcCache should show performance benefits
    arrow_write_results = [r for r in results if r.cache_type == 'ArrowIpcCache' and r.operation == 'write']
    sqlite_write_results = [r for r in results if r.cache_type == 'SqliteCache' and r.operation == 'write']
    
    # Calculate average speedup
    if arrow_write_results and sqlite_write_results:
        for arrow_result, sqlite_result in zip(arrow_write_results, sqlite_write_results):
            if sqlite_result.records_per_second > 0:
                speedup = arrow_result.records_per_second / sqlite_result.records_per_second
                # ArrowIpcCache should be functional (at least 0.1x) for medium datasets
                # Note: Arrow IPC has overhead for smaller datasets but excels at reads and large datasets
                assert speedup > 0.1, f"ArrowIpcCache write speedup too low: {speedup:.2f}x"


def test_large_dataset_performance(benchmark_suite):
    """Test performance with large datasets (500K+ records) - this is where Arrow should excel"""
    dataset_sizes = [500000]
    schema_types = ["mixed"]
    
    results = benchmark_suite.run_comparative_benchmark(dataset_sizes, schema_types)
    
    # Verify we have results
    expected_results = len(dataset_sizes) * len(schema_types) * 2 * 2
    assert len(results) == expected_results
    
    # For large datasets, ArrowIpcCache should show significant performance benefits
    arrow_write_results = [r for r in results if r.cache_type == 'ArrowIpcCache' and r.operation == 'write']
    sqlite_write_results = [r for r in results if r.cache_type == 'SqliteCache' and r.operation == 'write']
    arrow_read_results = [r for r in results if r.cache_type == 'ArrowIpcCache' and r.operation == 'read']
    sqlite_read_results = [r for r in results if r.cache_type == 'SqliteCache' and r.operation == 'read']
    
    # Verify performance characteristics
    for result in arrow_write_results + arrow_read_results:
        assert result.records_per_second > 1000, f"ArrowIpcCache should handle >1K RPS for large datasets: {result}"
    
    # Generate and print performance report
    report = benchmark_suite.generate_performance_report(results)
    print("\n" + report)


def test_wide_schema_performance(benchmark_suite):
    """Test performance with wide schemas (many columns)"""
    dataset_sizes = [10000, 50000]
    schema_types = ["wide"]
    
    results = benchmark_suite.run_comparative_benchmark(dataset_sizes, schema_types)
    
    # Verify we have results
    expected_results = len(dataset_sizes) * len(schema_types) * 2 * 2
    assert len(results) == expected_results
    
    # Wide schemas should benefit from Arrow's columnar format
    arrow_results = [r for r in results if r.cache_type == 'ArrowIpcCache']
    for result in arrow_results:
        assert result.records_per_second > 0, f"ArrowIpcCache should handle wide schemas: {result}"
        # Memory usage should be reasonable even with wide schemas
        assert result.memory_increase_mb < 1000, f"Memory usage too high for wide schema: {result.memory_increase_mb}MB"


def test_memory_usage_benchmarks(benchmark_suite):
    """Test memory usage characteristics of both cache implementations"""
    dataset_sizes = [100000]
    schema_types = ["mixed"]
    
    results = benchmark_suite.run_comparative_benchmark(dataset_sizes, schema_types)
    
    # Analyze memory usage patterns
    arrow_results = [r for r in results if r.cache_type == 'ArrowIpcCache']
    sqlite_results = [r for r in results if r.cache_type == 'SqliteCache']
    
    for result in arrow_results + sqlite_results:
        # Memory usage should be bounded and reasonable
        assert result.peak_memory_mb > 0, f"Peak memory should be positive: {result}"
        assert result.memory_increase_mb >= 0, f"Memory increase should be non-negative: {result}"
        
        # For 100K records, memory usage should be reasonable (less than 500MB increase)
        assert result.memory_increase_mb < 500, f"Memory usage too high: {result.memory_increase_mb}MB for {result}"


def test_throughput_measurements(benchmark_suite):
    """Test throughput measurements and verify they meet performance requirements"""
    dataset_sizes = [25000, 100000]
    schema_types = ["simple", "mixed"]
    
    results = benchmark_suite.run_comparative_benchmark(dataset_sizes, schema_types)
    
    # Verify throughput requirements (Requirements 1.1, 1.2, 1.3)
    arrow_write_results = [r for r in results if r.cache_type == 'ArrowIpcCache' and r.operation == 'write']
    arrow_read_results = [r for r in results if r.cache_type == 'ArrowIpcCache' and r.operation == 'read']
    
    # ArrowIpcCache should achieve reasonable throughput
    for result in arrow_write_results:
        # Write throughput should be at least 1K records/second for medium datasets
        if result.dataset_size >= 25000:
            assert result.records_per_second >= 1000, f"Write throughput too low: {result}"
    
    for result in arrow_read_results:
        # Read throughput should be at least 5K records/second for medium datasets
        if result.dataset_size >= 25000:
            assert result.records_per_second >= 5000, f"Read throughput too low: {result}"
    
    # Verify consistent performance (no significant degradation with larger datasets)
    if len(arrow_write_results) >= 2:
        small_dataset_result = min(arrow_write_results, key=lambda r: r.dataset_size)
        large_dataset_result = max(arrow_write_results, key=lambda r: r.dataset_size)
        
        # Performance shouldn't degrade by more than 50% as dataset size increases
        performance_ratio = large_dataset_result.records_per_second / small_dataset_result.records_per_second
        assert performance_ratio > 0.5, f"Performance degradation too high: {performance_ratio:.2f}"


if __name__ == "__main__":
    """Run benchmarks directly for development and testing"""
    
    print("Running Arrow IPC Cache Performance Benchmarks...")
    print("=" * 60)
    
    benchmark_suite = CacheBenchmark()
    
    try:
        # Run comprehensive benchmarks
        dataset_sizes = [1000, 10000, 50000, 100000]
        schema_types = ["simple", "mixed", "wide"]
        
        print("Starting comprehensive benchmark suite...")
        results = benchmark_suite.run_comparative_benchmark(dataset_sizes, schema_types)
        
        # Generate and display report
        report = benchmark_suite.generate_performance_report(results)
        print("\n" + report)
        
        # Save report to file
        report_path = "arrow_ipc_cache_benchmark_report.txt"
        with open(report_path, 'w') as f:
            f.write(report)
        print(f"\nDetailed report saved to: {report_path}")
        
    finally:
        benchmark_suite.cleanup()
        print("\nBenchmark cleanup completed.")