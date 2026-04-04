from core.versions import (
    EXTRACTION_VERSION, CHUNKING_VERSION,
    ENRICHMENT_VERSION, EMBEDDING_VERSION,
    get_processing_version
)

def test_all_4_constants_defined():
    assert EXTRACTION_VERSION
    assert CHUNKING_VERSION
    assert ENRICHMENT_VERSION
    assert EMBEDDING_VERSION

def test_get_processing_version_composite_string():
    result = get_processing_version()
    assert result == 'extract-v1.0|chunk-v1.0|enrich-v1.0|bge-m3-v1.0'

def test_version_change_changes_composite():
    import core.versions as v
    original = v.EXTRACTION_VERSION
    v.EXTRACTION_VERSION = 'extract-v2.0'
    result = get_processing_version()
    assert 'extract-v2.0' in result
    v.EXTRACTION_VERSION = original
