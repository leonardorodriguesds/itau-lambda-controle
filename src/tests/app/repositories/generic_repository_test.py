import pytest
from unittest.mock import Mock, MagicMock
from sqlalchemy import Column, DateTime, Integer
from sqlalchemy.orm import Session
from logging import Logger
from datetime import datetime

from src.app.repositories.generic_repository import GenericRepository

class TestGenericRepository:
    @pytest.fixture
    def mock_session(self):
        return Mock(spec=Session)

    @pytest.fixture
    def mock_logger(self):
        return Mock(spec=Logger)

    @pytest.fixture
    def mock_model(self):
        class MockModel:
            id = Column(Integer, primary_key=True)
            date_deleted = Column(DateTime, nullable=True)
        return MockModel


    @pytest.fixture
    def repository(self, mock_session, mock_model, mock_logger):
        return GenericRepository(mock_session, mock_model, mock_logger)

    def test_save_new_object(self, repository, mock_session, mock_logger, mock_model):
        obj = mock_model()
        obj.id = None

        repository.save(obj)

        mock_session.add.assert_called_once_with(obj)
        mock_session.flush.assert_called_once()
        mock_logger.debug.assert_any_call(f"[{repository.__class__.__name__}] Saving object: [{obj.__dict__}]")

    def test_save_existing_object(self, repository, mock_session, mock_logger, mock_model):
        obj = mock_model()
        obj.id = 1

        repository.save(obj)

        mock_session.merge.assert_called_once_with(obj)
        mock_session.flush.assert_called_once()

    def test_get_by_id(self, repository, mock_session, mock_model):
        mock_query = Mock()
        mock_session.query.return_value.filter.return_value = mock_query
        mock_query.first.return_value = "mocked_object"

        result = repository.get_by_id(1)

        mock_session.query.assert_called_once_with(mock_model)
        mock_session.query.return_value.filter.assert_called_once()
        assert result == "mocked_object"

    def test_get_all(self, repository, mock_session, mock_model):
        mock_query = Mock()
        mock_session.query.return_value.filter.return_value = mock_query
        mock_query.all.return_value = ["object1", "object2"]

        result = repository.get_all()

        mock_session.query.assert_called_once_with(mock_model)
        mock_session.query.return_value.filter.assert_called_once()
        assert result == ["object1", "object2"]

    def test_update(self, repository, mock_session, mock_model):
        obj = mock_model()
        repository.get_by_id = Mock(return_value=obj)

        merged_obj = Mock()
        mock_session.merge.return_value = merged_obj

        updated_data = {"field": "value"}
        result = repository.update(1, updated_data)

        mock_session.merge.assert_called_once_with(obj)
        mock_session.flush.assert_called_once()

        assert result == merged_obj


    def test_hard_delete(self, repository, mock_session, mock_model):
        obj = mock_model()
        repository.get_by_id = Mock(return_value=obj)

        result = repository.hard_delete(1)

        mock_session.delete.assert_called_once_with(obj)
        assert result is True

    def test_soft_delete(self, repository, mock_session, mock_model):
        obj = mock_model()
        repository.get_by_id = Mock(return_value=obj)

        result = repository.soft_delete(1)

        assert obj.date_deleted is not None
        mock_session.merge.assert_called_once_with(obj)
        mock_session.flush.assert_called_once()
        assert result is True

    def test_hard_delete_not_found(self, repository, mock_session, mock_model):
        repository.get_by_id = Mock(return_value=None)

        result = repository.hard_delete(1)

        mock_session.delete.assert_not_called()
        assert result is False
        
    def test_soft_delete_not_found(self, repository, mock_session, mock_model):
        repository.get_by_id = Mock(return_value=None)

        result = repository.soft_delete(1)

        mock_session.merge.assert_not_called()
        assert result is False
        
    def test_save_with_id_should_call_update(self, repository, mock_session, mock_model):
        obj = mock_model()
        obj.id = 1

        repository.get_by_id = Mock(return_value=obj)
        repository.save(obj)

        mock_session.merge.assert_called_once_with(obj)
        mock_session.flush.assert_called_once()
        
    def test_save_without_id_should_call_add(self, repository, mock_session, mock_model):
        obj = mock_model()
        obj.id = None

        repository.save(obj)

        mock_session.add.assert_called_once_with(obj)
        mock_session.flush.assert_called_once()
        
    def test_get_by_id_with_date_deleted(self, repository, mock_session, mock_model):
        mock_query = Mock()
        mock_model.date_deleted = Column(DateTime, nullable=True)
        mock_session.query.return_value.filter.return_value = mock_query
        mock_query.first.return_value = None

        result = repository.get_by_id(1)

        mock_session.query.assert_called_once_with(mock_model)
        mock_session.query.return_value.filter.assert_called_once()
        assert result is None
        
    def test_get_all_with_date_deleted(self, repository, mock_session, mock_model):
        mock_query = Mock()
        mock_model.date_deleted = Column(DateTime, nullable=True)
        mock_session.query.return_value.filter.return_value = mock_query
        mock_query.all.return_value = []

        result = repository.get_all()

        mock_session.query.assert_called_once_with(mock_model)
        mock_session.query.return_value.filter.assert_called_once()
        assert result == []