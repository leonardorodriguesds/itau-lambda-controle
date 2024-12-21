import pytest
from unittest.mock import MagicMock
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, DateTime, create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from src.app.models.base import Base 
from src.app.repositories.generic_repository import GenericRepository
from aws_lambda_powertools.event_handler.exceptions import NotFoundError

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    date_deleted = Column(DateTime, nullable=True)
    
    posts = relationship("Post", back_populates="user")

    def __repr__(self):
        return f"<User(id={self.id}, name='{self.name}', email='{self.email}')>"

class Post(Base):
    __tablename__ = 'posts'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    content = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    date_deleted = Column(DateTime, nullable=True)
    
    user = relationship("User", back_populates="posts")

    def __repr__(self):
        return f"<Post(id={self.id}, title='{self.title}', user_id={self.user_id})>"

@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)

    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    yield session

    session.close()
    Base.metadata.drop_all(engine)

@pytest.fixture
def mock_logger(mocker):
    return mocker.MagicMock()

@pytest.fixture
def user_repository(db_session, mock_logger):
    return GenericRepository[User](db_session=db_session, model=User, logger=mock_logger)

@pytest.fixture
def post_repository(db_session, mock_logger):
    return GenericRepository[Post](db_session=db_session, model=Post, logger=mock_logger)

def test_save_new_object(user_repository, db_session, mock_logger):
    new_user = User(name="John Doe", email="john@example.com")
    saved_user = user_repository.save(new_user)
    
    assert saved_user.id is not None
    assert saved_user.name == "John Doe"
    assert saved_user.email == "john@example.com"
    
    retrieved_user = db_session.query(User).filter_by(id=saved_user.id).first()
    assert retrieved_user is not None
    assert retrieved_user.name == "John Doe"

def test_save_existing_object(user_repository, db_session, mock_logger):
    existing_user = User(name="Jane Doe", email="jane@example.com")
    db_session.add(existing_user)
    db_session.flush()
    user_id = existing_user.id

    existing_user.name = "Jane Smith"
    saved_user = user_repository.save(existing_user)

    assert saved_user.id == user_id
    assert saved_user.name == "Jane Smith"

    retrieved_user = db_session.query(User).filter_by(id=user_id).first()
    assert retrieved_user.name == "Jane Smith"

def test_get_by_id_found(user_repository, db_session, mock_logger):
    user = User(name="Alice", email="alice@example.com")
    db_session.add(user)
    db_session.flush()
    user_id = user.id

    result = user_repository.get_by_id(user_id)

    assert result is not None
    assert result.id == user_id
    assert result.name == "Alice"

def test_get_by_id_not_found(user_repository, db_session, mock_logger):
    result = user_repository.get_by_id(999)

    assert result is None

def test_get_all(user_repository, db_session, mock_logger):
    users = [
        User(name="User1", email="user1@example.com"),
        User(name="User2", email="user2@example.com"),
        User(name="User3", email="user3@example.com", date_deleted=datetime.now())
    ]
    db_session.add_all(users)
    db_session.flush()

    result = user_repository.get_all()

    assert len(result) == 2
    assert all(user.date_deleted is None for user in result)

def test_update_existing_object(user_repository, db_session, mock_logger):
    user = User(name="Bob", email="bob@example.com")
    db_session.add(user)
    db_session.flush()
    user_id = user.id

    updated_data = {"name": "Bobby", "email": "bobby@example.com"}
    updated_user = user_repository.update(user_id, updated_data)

    assert updated_user is not None
    assert updated_user.name == "Bobby"
    assert updated_user.email == "bobby@example.com"

    retrieved_user = db_session.query(User).filter_by(id=user_id).first()
    assert retrieved_user.name == "Bobby"
    assert retrieved_user.email == "bobby@example.com"

def test_update_nonexistent_object(user_repository, db_session, mock_logger):
    updated_data = {"name": "Nonexistent"}
    result = user_repository.update(999, updated_data)

    assert result is None

def test_hard_delete_existing_object(user_repository, db_session, mock_logger):
    user = User(name="Charlie", email="charlie@example.com")
    db_session.add(user)
    db_session.flush()
    user_id = user.id

    result = user_repository.hard_delete(user_id)

    assert result is True

    retrieved_user = db_session.query(User).filter_by(id=user_id).first()
    assert retrieved_user is None

def test_hard_delete_nonexistent_object(user_repository, db_session, mock_logger):
    result = user_repository.hard_delete(999)

    assert result is False

def test_soft_delete_existing_object(user_repository, db_session, mock_logger):
    user = User(name="Diana", email="diana@example.com")
    db_session.add(user)
    db_session.flush()
    user_id = user.id

    result = user_repository.soft_delete(user_id)

    assert result is True

    retrieved_user = db_session.query(User).filter_by(id=user_id).first()
    assert retrieved_user is not None
    assert retrieved_user.date_deleted is not None

def test_soft_delete_nonexistent_object(user_repository, db_session, mock_logger):
    result = user_repository.soft_delete(999)

    assert result is False

def test_query_with_filters(user_repository, db_session, mock_logger):
    users = [
        User(name="Eve", email="eve@example.com"),
        User(name="Eve", email="eve2@example.com"),
        User(name="Frank", email="frank@example.com")
    ]
    db_session.add_all(users)
    db_session.flush()

    posts = [
        Post(title="Post1", content="Content1", user_id=users[0].id),
        Post(title="Post2", content="Content2", user_id=users[0].id, date_deleted=datetime.now()),
        Post(title="Post3", content="Content3", user_id=users[1].id)
    ]
    db_session.add_all(posts)
    db_session.flush()

    filters = {"name": "Eve"}
    result = user_repository.query(**filters)

    assert len(result) == 2
    assert all(user.name == "Eve" for user in result)

def test_flush(user_repository, db_session, mock_logger):
    user = User(name="Grace", email="grace@example.com")
    db_session.add(user)

    user_repository.flush()

    retrieved_user = db_session.query(User).filter_by(email="grace@example.com").first()
    assert retrieved_user is not None

def test_save_exception(user_repository, db_session, mock_logger):
    user1 = User(name="Henry", email="henry@example.com")
    db_session.add(user1)
    db_session.flush()

    user2 = User(name="Henry II", email="henry@example.com")

    with pytest.raises(Exception) as exc_info:
        user_repository.save(user2)

    assert "UNIQUE constraint failed" in str(exc_info.value)

    mock_logger.error.assert_called()
    error_call_args = mock_logger.error.call_args[0][0]
    assert "Error saving object" in error_call_args

def test_get_by_id_exception(user_repository, db_session, mock_logger, mocker):
    mocker.patch.object(db_session, 'query', side_effect=NotFoundError("Query error"))

    with pytest.raises(NotFoundError):
        user_repository.get_by_id(1)

def test_complex_query(post_repository, db_session, mock_logger):
    user1 = User(name="User1", email="user1@example.com")
    user2 = User(name="User2", email="user2@example.com")
    user3 = User(name="User3", email="user3@example.com", date_deleted=datetime.now())  

    db_session.add_all([user1, user2, user3])
    db_session.flush()

    post1 = Post(title="Post1", content="Content1", user_id=user1.id)
    post2 = Post(title="Post2", content="Content2", user_id=user1.id, date_deleted=datetime.now())  
    post3 = Post(title="Post3", content="Content3", user_id=user2.id)
    post4 = Post(title="Post4", content="Content4", user_id=user2.id)
    post5 = Post(title="Post5", content="Content5", user_id=user3.id)

    db_session.add_all([post1, post2, post3, post4, post5])
    db_session.flush()

    filters = {
        "user.name": "User1",
        "title": "Post1"
    }

    result = post_repository.query(**filters)

    assert len(result) == 1
    assert result[0].title == "Post1"
    assert result[0].user.name == "User1"

def test_get_by_id_soft_deleted(user_repository, db_session, mock_logger):
    user = User(name="SoftDeletedUser", email="softdeleted@example.com", date_deleted=datetime.now())
    db_session.add(user)
    db_session.flush()
    user_id = user.id

    result = user_repository.get_by_id(user_id)

    assert result is None

def test_query_excludes_soft_deleted(post_repository, db_session, mock_logger):
    user1 = User(name="ActiveUser", email="active@example.com")
    user2 = User(name="SoftDeletedUser", email="softdeleted@example.com", date_deleted=datetime.now())

    db_session.add_all([user1, user2])
    db_session.flush()

    post1 = Post(title="ActivePost", content="Active Content", user_id=user1.id)
    post2 = Post(title="SoftDeletedPost", content="Soft Deleted Content", user_id=user1.id, date_deleted=datetime.now())

    db_session.add_all([post1, post2])
    db_session.flush()

    result = post_repository.query()

    assert len(result) == 1
    assert result[0].title == "ActivePost"
