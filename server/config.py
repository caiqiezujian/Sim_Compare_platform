"""Runtime configuration for SimCompare.

The platform can run with only request-time values from the UI, but production
deployments usually need stable defaults for gRPC endpoints and debug file
locations.  This module loads one JSON file at backend startup and keeps
environment variables as an override layer.  The same file can be updated at
runtime via the ``PUT /api/config`` endpoint, which then re-reads the file
into the in-memory ``CONFIG`` so the rest of the app sees the new values
without a restart.
"""
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = ROOT_DIR / "simcompare.config.json"

# Internal keys prefixed with "_" that we strip before writing the file back.
_META_KEYS = ("_path", "_loaded")


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


# Use a single mutable dict so reload_config() can update it in-place and any
# module that already imported `CONFIG` keeps seeing the new contents.
CONFIG: Dict[str, Any] = _load_config()


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


def glossary_config() -> list:
    value = CONFIG.get("glossary")
    return value if isinstance(value, list) else []


def ner_config() -> Dict[str, Any]:
    value = CONFIG.get("ner")
    return value if isinstance(value, dict) else {}


def reload_config() -> Dict[str, Any]:
    """Re-read the config file from disk into the in-memory ``CONFIG`` dict."""
    fresh = _load_config()
    CONFIG.clear()
    CONFIG.update(fresh)
    return CONFIG


def save_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Validate, persist, and reload ``payload`` as the active config.

    ``payload`` may include any subset of the top-level keys (``services``,
    ``runtime``, ``storage``).  Internal ``_*`` keys are stripped.  The file
    is written atomically: a temp file in the same directory, then ``os.replace``
    so a crash mid-write can never leave a half-written config on disk.
    """
    if not isinstance(payload, dict):
        raise ValueError("config payload must be a JSON object")
    cleaned: Dict[str, Any] = {key: value for key, value in payload.items() if not key.startswith("_")}
    path = Path(config_path())
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".simcompare.config.", suffix=".json", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as writer:
            json.dump(cleaned, writer, ensure_ascii=False, indent=2)
            writer.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        # Best-effort cleanup of the temp file if the rename failed.
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    return reload_config()
