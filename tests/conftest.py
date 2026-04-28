"""pytest conftest — make the project importable without `pip install`.

We run tests directly out of the source tree before the package is
installed, so prepend the project root to sys.path.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
