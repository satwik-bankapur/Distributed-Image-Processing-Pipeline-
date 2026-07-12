"""Put the master and worker package roots on sys.path so tests can import both."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for pkg_root in (ROOT / "master", ROOT / "worker"):
    sys.path.insert(0, str(pkg_root))
