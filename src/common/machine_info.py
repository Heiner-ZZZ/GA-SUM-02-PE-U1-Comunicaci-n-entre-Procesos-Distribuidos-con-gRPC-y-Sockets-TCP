from __future__ import annotations

import ctypes
import importlib.metadata
import json
import os
import platform
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from .config import DATA_DIR

TRACKED_PACKAGES = (
    "grpcio",
    "grpcio-tools",
    "protobuf",
    "pandas",
    "matplotlib",
    "numpy",
)


def _get_total_ram_gb() -> Optional[float]:
    if sys.platform == "win32":
        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = MEMORYSTATUSEX()
        status.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return round(status.ullTotalPhys / (1024 ** 3), 2)
        return None

    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        return round((pages * page_size) / (1024 ** 3), 2)
    except (AttributeError, ValueError, OSError):
        return None


def _get_package_versions() -> Dict[str, str]:
    versions: Dict[str, str] = {}
    for pkg in TRACKED_PACKAGES:
        try:
            versions[pkg] = importlib.metadata.version(pkg)
        except importlib.metadata.PackageNotFoundError:
            versions[pkg] = "no-instalado"
    return versions


def collect_machine_info(execution_mode: str = "procesos locales en localhost") -> Dict[str, Any]:
    return {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "captured_at_local": datetime.now().astimezone().isoformat(),
        "hostname": socket.gethostname(),
        "os_system": platform.system(),
        "os_release": platform.release(),
        "os_version": platform.version(),
        "architecture": platform.machine(),
        "processor": platform.processor(),
        "cpu_count_logical": os.cpu_count(),
        "ram_total_gb": _get_total_ram_gb(),
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "execution_mode": execution_mode,
        "packages": _get_package_versions(),
    }


def save_machine_info(output_path: Optional[Path] = None) -> Path:
    target = output_path or (DATA_DIR / "machine_info.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    info = collect_machine_info()
    target.write_text(json.dumps(info, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return target


if __name__ == "__main__":
    print(json.dumps(collect_machine_info(), indent=2, ensure_ascii=False))
