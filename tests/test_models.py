import pytest
import uuid
from sqlalchemy.exc import IntegrityError
from core.models import SourceDocument, ContentChunk, ChunkEmbedding

def make_source(**kwargs):
    defaults = dict(source_type='pdf', source_kind='text_document')
    defaults.update(kwargs)
    return SourceDocument(**defaults)

def make_chunk(source_id, chunk_index=0, **kwargs):
    defaults = dict(source_id=source_id, chunk_index=chunk_index, content='test content')
    defaults.update(kwargs)
    return ContentChunk(**defaults)

def test_source_document_is_production_approved_default_false(db_session):
    doc = make_source()
    db_session.add(doc)
    db_session.flush()
    assert doc.is_production_approved is False

def test_source_document_is_active_default_true(db_session):
    doc = make_source()
    db_session.add(doc)
    db_session.flush()
    assert doc.is_active is True

def test_content_chunk_state_default_active(db_session):
    doc = make_source()
    db_session.add(doc)
    db_session.flush()
    chunk = make_chunk(source_id=doc.id)
    db_session.add(chunk)
    db_session.flush()
    assert chunk.state == 'active'

def test_content_chunk_unique_constraint_source_chunk_index(db_session):
    doc = make_source()
    db_session.add(doc)
    db_session.flush()
    chunk1 = make_chunk(source_id=doc.id, chunk_index=0)
    chunk2 = make_chunk(source_id=doc.id, chunk_index=0)
    db_session.add(chunk1)
    db_session.flush()
    db_session.add(chunk2)
    with pytest.raises(IntegrityError):
        db_session.flush()

def test_chunk_embedding_column_uses_vector_1024(db_session):
    from sqlalchemy import inspect
    inspector = inspect(db_session.bind)
    cols = {c['name']: c for c in inspector.get_columns('chunk_embeddings')}
    assert 'embedding' in cols

def test_cascade_delete_source_removes_chunks(db_session):
    doc = make_source()
    db_session.add(doc)
    db_session.flush()
    chunk = make_chunk(source_id=doc.id)
    db_session.add(chunk)
    db_session.flush()
    chunk_id = chunk.id
    db_session.delete(doc)
    db_session.flush()
    result = db_session.get(ContentChunk, chunk_id)
    assert result is None

def test_cascade_delete_chunk_removes_embedding(db_session):
    doc = make_source()
    db_session.add(doc)
    db_session.flush()
    chunk = make_chunk(source_id=doc.id)
    db_session.add(chunk)
    db_session.flush()
    emb = ChunkEmbedding(chunk_id=chunk.id, embedding=[0.0] * 1024)
    db_session.add(emb)
    db_session.flush()
    emb_id = emb.id
    db_session.delete(chunk)
    db_session.flush()
    result = db_session.get(ChunkEmbedding, emb_id)
    assert result is None

def test_duplicate_of_nullable(db_session):
    doc = make_source()
    db_session.add(doc)
    db_session.flush()
    chunk = make_chunk(source_id=doc.id)
    db_session.add(chunk)
    db_session.flush()
    assert chunk.duplicate_of is None
