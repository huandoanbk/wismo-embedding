import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

@pytest.fixture(scope='session')
def engine():
    url = os.environ.get('VECTOR_DB_URL')
    eng = create_engine(url)
    yield eng

@pytest.fixture
def db_session(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.rollback()
    session.close()
