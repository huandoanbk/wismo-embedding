"""Connector package for source intake adapters."""

from .base import SourceCandidate, SourceConnector
from .contabo_connector import ContaboConnector
from .downloadable_connector import DownloadableConnector, UnsupportedDynamicPageError
from .manual_upload_connector import ManualUploadConnector

__all__ = [
    "ContaboConnector",
    "DownloadableConnector",
    "ManualUploadConnector",
    "SourceCandidate",
    "SourceConnector",
    "UnsupportedDynamicPageError",
]
