"""Dashboard package for admin and ops surfaces."""

import sys
from pathlib import Path

# Ensure repo root is on sys.path when Streamlit executes page files directly.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
