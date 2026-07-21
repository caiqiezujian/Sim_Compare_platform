"""Runtime configuration for SimCompare.

The platform can run with only request-time values from the UI, but production
deployments usually need stable defaults for gRPC endpoints and debug file
locations.  This module loads one JSON file at backend startup and keeps
environment variables as an override layer.
"""
import json
import os
from pathlib import Path
from typing import Any, Dict

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = ROOT_DIR / "simcompare.config.json"


def _load_config() -> Dict[str, Any]:
    configured_path = os.getenv("SIMCOMPARE_CONFIG")
    path = Path(configured_path).expanduser() if configured_path else DEFAULT_CONFIG_PATH
    if not path.exists():
        return {"_path": str(path), "_loaded": False}
    with path.open("r", encoding="utf-8") as reader:
        data = json.load(reader)
    if not isinstance(data, dict):
        raise ValueError(f"SimCompare config must be a JSON object: {path}")
    data["_path"] = str(path)
    data["_loaded"] = True
    return data


CONFIG = _load_config()


def config_loaded() -> bool:
    return bool(CONFIG.get("_loaded"))


def config_path() -> str:
    return str(CONFIG.get("_path") or DEFAULT_CONFIG_PATH)


def service_config(side: str) -> Dict[str, Any]:
    services = CONFIG.get("services") or {}
    value = services.get(side.lower()) or {}
    return value if isinstance(value, dict) else {}


def runtime_config() -> Dict[str, Any]:
    value = CONFIG.get("runtime") or {}
    return value if isinstance(value, dict) else {}


def storage_config() -> Dict[str, Any]:
    value = CONFIG.get("storage") or {}
    return value if isinstance(value, dict) else {}
