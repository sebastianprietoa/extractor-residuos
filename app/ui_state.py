from __future__ import annotations

from typing import Any, MutableMapping


FOLDER_PATH_KEY = "selected_folder_path"
UPLOADER_NONCE_KEY = "uploader_nonce"


def ensure_selection_state(state: MutableMapping[str, Any]) -> None:
    state.setdefault(FOLDER_PATH_KEY, "")
    state.setdefault(UPLOADER_NONCE_KEY, 0)


def get_selected_folder(state: MutableMapping[str, Any], key: str = FOLDER_PATH_KEY) -> str:
    value = state.get(key, "")
    return str(value).strip()


def set_selected_folder(
    state: MutableMapping[str, Any],
    folder_path: str,
    key: str = FOLDER_PATH_KEY,
) -> None:
    state[key] = folder_path.strip()


def clear_selected_folder(state: MutableMapping[str, Any], key: str = FOLDER_PATH_KEY) -> None:
    state[key] = ""


def clear_selection(
    state: MutableMapping[str, Any],
    folder_key: str = FOLDER_PATH_KEY,
    uploader_key: str = UPLOADER_NONCE_KEY,
) -> None:
    state[uploader_key] = int(state.get(uploader_key, 0) or 0) + 1
    state[folder_key] = ""
