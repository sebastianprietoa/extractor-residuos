from __future__ import annotations

import importlib
import sys
import tempfile
import types
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch


class _UploadLike:
    def __init__(self, relative_name: str, payload: bytes = b"%PDF-1.4\n%test") -> None:
        self.filename = relative_name
        self.name = relative_name
        self.content_type = "application/pdf"
        self.file = BytesIO(payload)
        self._payload = payload

    def getbuffer(self):
        return memoryview(self._payload)


def _build_app_main_stubs() -> dict[str, types.ModuleType]:
    fastapi = types.ModuleType("fastapi")
    fastapi.__path__ = []  # type: ignore[attr-defined]

    class HTTPException(Exception):
        def __init__(self, status_code: int, detail: str) -> None:
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class FastAPI:
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs
            self.version = kwargs.get("version")

        def get(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

        def post(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

    def File(*args, **kwargs):
        return None

    class UploadFile:
        pass

    fastapi.FastAPI = FastAPI
    fastapi.UploadFile = UploadFile
    fastapi.File = File
    fastapi.HTTPException = HTTPException

    responses = types.ModuleType("fastapi.responses")
    responses.FileResponse = object
    responses.HTMLResponse = object
    fastapi.responses = responses

    starlette = types.ModuleType("starlette")
    starlette.__path__ = []  # type: ignore[attr-defined]

    background = types.ModuleType("starlette.background")

    class BackgroundTask:
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs

    background.BackgroundTask = BackgroundTask
    starlette.background = background

    def _stub_process_folder(*args, **kwargs):
        return None

    app_sinader = types.ModuleType("app.sinader")
    app_sinader.process_folder = _stub_process_folder
    app_sindrep = types.ModuleType("app.sindrep")
    app_sindrep.process_folder = _stub_process_folder
    app_simapro = types.ModuleType("app.simapro")
    app_simapro.process_folder = _stub_process_folder

    return {
        "fastapi": fastapi,
        "fastapi.responses": responses,
        "starlette": starlette,
        "starlette.background": background,
        "app.sinader": app_sinader,
        "app.sindrep": app_sindrep,
        "app.simapro": app_simapro,
    }


def _build_streamlit_stubs() -> dict[str, types.ModuleType]:
    streamlit = types.ModuleType("streamlit")
    streamlit.set_page_config = lambda *args, **kwargs: None

    def _stub_process_folder(*args, **kwargs):
        return None

    app_autocontrol = types.ModuleType("app.autocontrol")
    app_autocontrol.process_folder = _stub_process_folder
    app_sinader = types.ModuleType("app.sinader")
    app_sinader.process_folder = _stub_process_folder
    app_sindrep = types.ModuleType("app.sindrep")
    app_sindrep.process_folder = _stub_process_folder
    app_simapro = types.ModuleType("app.simapro")
    app_simapro.process_folder = _stub_process_folder

    return {
        "streamlit": streamlit,
        "app.autocontrol": app_autocontrol,
        "app.sinader": app_sinader,
        "app.sindrep": app_sindrep,
        "app.simapro": app_simapro,
    }


class UploadPathPreservationTests(unittest.TestCase):
    def test_fastapi_upload_preserves_parent_folder(self) -> None:
        stubs = _build_app_main_stubs()
        with patch.dict(sys.modules, stubs, clear=False):
            sys.modules.pop("app.main", None)
            app_main = importlib.import_module("app.main")

            with tempfile.TemporaryDirectory(prefix="main_upload_") as temp_dir:
                input_dir = Path(temp_dir) / "input"
                input_dir.mkdir(parents=True, exist_ok=True)

                uploads = [
                    _UploadLike("Ballenas 02/SIDREP 1887318 ABIERTO.pdf"),
                    _UploadLike("Ballenas 02/SIDREP 1887318 CERRADO.pdf"),
                    _UploadLike("Erasmo 02/declaracion1877892_24432.pdf"),
                ]

                saved = app_main.save_uploaded_pdfs(uploads, input_dir)
                saved_paths = list(input_dir.rglob("*.pdf"))

        self.assertEqual(saved, 3)
        self.assertEqual(len(saved_paths), 3)
        self.assertTrue(
            any(
                p.parent.name == "Ballenas 02" and p.name.endswith("SIDREP 1887318 ABIERTO.pdf")
                for p in saved_paths
            )
        )
        self.assertTrue(
            any(
                p.parent.name == "Ballenas 02" and p.name.endswith("SIDREP 1887318 CERRADO.pdf")
                for p in saved_paths
            )
        )
        self.assertTrue(
            any(
                p.parent.name == "Erasmo 02" and p.name.endswith("declaracion1877892_24432.pdf")
                for p in saved_paths
            )
        )

    def test_streamlit_upload_preserves_parent_folder(self) -> None:
        stubs = _build_streamlit_stubs()
        with patch.dict(sys.modules, stubs, clear=False):
            sys.modules.pop("app.streamlit_app", None)
            streamlit_app = importlib.import_module("app.streamlit_app")

            with tempfile.TemporaryDirectory(prefix="streamlit_upload_") as temp_dir:
                input_dir = Path(temp_dir) / "input"
                input_dir.mkdir(parents=True, exist_ok=True)

                uploads = [
                    _UploadLike("Ballenas 02/SIDREP 1887318 ABIERTO.pdf"),
                    _UploadLike("Erasmo 02/declaracion1877892_24432.pdf"),
                ]

                saved = streamlit_app._save_uploads(uploads, input_dir)
                saved_paths = list(input_dir.rglob("*.pdf"))

        self.assertEqual(saved, 2)
        self.assertEqual(len(saved_paths), 2)
        self.assertTrue(
            any(
                p.parent.name == "Ballenas 02" and p.name.endswith("SIDREP 1887318 ABIERTO.pdf")
                for p in saved_paths
            )
        )
        self.assertTrue(
            any(
                p.parent.name == "Erasmo 02" and p.name.endswith("declaracion1877892_24432.pdf")
                for p in saved_paths
            )
        )


if __name__ == "__main__":
    unittest.main()
