import sys
from pathlib import Path

_GRPC_DIR = str(Path(__file__).resolve().parent)
if _GRPC_DIR not in sys.path:
    sys.path.insert(0, _GRPC_DIR)
