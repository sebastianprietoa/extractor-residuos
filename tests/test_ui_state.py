from __future__ import annotations

import unittest

from app.ui_state import (
    clear_selection,
    clear_selected_folder,
    ensure_selection_state,
    get_selected_folder,
    set_selected_folder,
)


class UiStateTests(unittest.TestCase):
    def test_folder_selection_can_be_set_and_cleared(self) -> None:
        state: dict[str, object] = {}

        ensure_selection_state(state)
        self.assertEqual(state["selected_folder_path"], "")
        self.assertEqual(state["uploader_nonce"], 0)

        set_selected_folder(state, "  /mnt/d/proyecto/carpeta  ")
        self.assertEqual(get_selected_folder(state), "/mnt/d/proyecto/carpeta")

        clear_selected_folder(state)
        self.assertEqual(get_selected_folder(state), "")

    def test_clear_selection_resets_folder_and_nonce(self) -> None:
        state: dict[str, object] = {"selected_folder_path": "/tmp/demo", "uploader_nonce": 7}

        clear_selection(state)

        self.assertEqual(state["selected_folder_path"], "")
        self.assertEqual(state["uploader_nonce"], 8)


if __name__ == "__main__":
    unittest.main()
