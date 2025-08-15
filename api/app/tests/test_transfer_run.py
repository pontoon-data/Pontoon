import uuid
from unittest.mock import Mock, patch
import pytest
from sqlmodel import Session

from app.models.transfer_run import TransferRun
from app.models.transfer import Transfer


class TestTransferRun:
    """Test cases for TransferRun model"""

    def test_get_transfer_row_count_normal_operation(self):
        """Test get_transfer_row_count with various valid progress data scenarios"""
        mock_session = Mock()
        
        # Test 1: Mixed source and destination operations
        mock_transfer_run_1 = Mock()
        mock_transfer_run_1.output = {
            'progress': {
                'source_1': {
                    'entity': 'source_1',
                    'processed': 1000,
                    'total': 1000,
                    'percent': 100.0
                },
                'destination_1': {
                    'entity': 'destination_1',
                    'processed': 1000,
                    'total': 1000,
                    'percent': 100.0
                }
            }
        }
        
        with patch.object(TransferRun, 'get', return_value=mock_transfer_run_1):
            result = TransferRun.get_transfer_row_count(mock_session, uuid.uuid4())
        assert result == 1000  # Only destination rows counted
        
    def test_get_transfer_row_count_zero_rows_transferred(self):
        mock_session = Mock()
        mock_transfer_run_3 = Mock()
        mock_transfer_run_3.output = {
            'progress': {
                'source_1': {'entity': 'source_1', 'processed': 100},
                'destination_1': {'entity': 'destination_1', 'processed': 0}
            }
        }
        
        with patch.object(TransferRun, 'get', return_value=mock_transfer_run_3):
            result = TransferRun.get_transfer_row_count(mock_session, uuid.uuid4())
        assert result == 0  # 0 destination rows

    def test_get_transfer_row_count_edge_cases(self):
        """Test get_transfer_row_count with edge cases and malformed data - returns None only when no progress data exists"""
        mock_session = Mock()
        
        # Test 1: No output field
        mock_transfer_run_1 = Mock()
        mock_transfer_run_1.output = None
        
        with patch.object(TransferRun, 'get', return_value=mock_transfer_run_1):
            result = TransferRun.get_transfer_row_count(mock_session, uuid.uuid4())
        assert result is None
        
        # Test 2: No progress field
        mock_transfer_run_2 = Mock()
        mock_transfer_run_2.output = {'some_other_field': 'value'}
        
        with patch.object(TransferRun, 'get', return_value=mock_transfer_run_2):
            result = TransferRun.get_transfer_row_count(mock_session, uuid.uuid4())
        assert result is None

        # Test 3: Empty progress field
        mock_transfer_run_3 = Mock()
        mock_transfer_run_3.output = {'some_other_field': 'value', 'progress': {}}
        
        with patch.object(TransferRun, 'get', return_value=mock_transfer_run_3):
            result = TransferRun.get_transfer_row_count(mock_session, uuid.uuid4())
        assert result is None