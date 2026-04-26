EXTRACTION_VERSION  = 'extract-v1.0'
CHUNKING_VERSION    = 'chunk-v1.0'
ENRICHMENT_VERSION  = 'enrich-v1.0'
EMBEDDING_VERSION   = 'bge-m3-v1.0'


def get_processing_version(*, include_enrichment: bool = True) -> str:
    if include_enrichment:
        return f"{EXTRACTION_VERSION}|{CHUNKING_VERSION}|{ENRICHMENT_VERSION}|{EMBEDDING_VERSION}"
    return f"{EXTRACTION_VERSION}|{CHUNKING_VERSION}|no-enrich|{EMBEDDING_VERSION}"
