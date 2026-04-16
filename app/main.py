from pathlib import Path
import shutil
import tempfile
from typing import Annotated, List
from uuid import uuid4

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

from app.sinader import process_folder as process_sinader
from app.sindrep import process_folder as process_sindrep

app = FastAPI(
    title="Extractor de Certificados",
    version="1.0.0",
    description=(
        "API para procesar certificados PDF de SINADER y SINDREP.\n\n"
        "Sube uno o más PDF por endpoint y recibirás un archivo Excel con los datos extraídos."
    ),
)

ALLOWED_PDF_CONTENT_TYPES = {
    "",
    "application/octet-stream",
    "application/pdf",
    "application/x-pdf",
    "binary/octet-stream",
}

ALLOWED_PDF_CONTENT_TYPES = {
    "",
    "application/octet-stream",
    "application/pdf",
    "application/x-pdf",
    "binary/octet-stream",
}


def cleanup_temp_dir(temp_dir: str) -> None:
    shutil.rmtree(temp_dir, ignore_errors=True)


def save_uploaded_pdfs(files: List[UploadFile], input_dir: Path) -> int:
    saved_count = 0

    for idx, uploaded in enumerate(files, start=1):
        if not uploaded.filename:
            continue

        original_name = Path(uploaded.filename).name
        suffix = Path(original_name).suffix.lower()

        if suffix != ".pdf":
            raise HTTPException(
                status_code=400,
                detail=f"El archivo '{original_name}' no es un PDF válido"
            )

        content_type = (uploaded.content_type or "").lower()
        if content_type not in ALLOWED_PDF_CONTENT_TYPES:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"El archivo '{original_name}' no tiene un content-type válido de PDF "
                    f"({content_type or 'vacío'})"
                ),
            )

        safe_name = f"{idx:03d}_{uuid4().hex}_{original_name}"
        dst = input_dir / safe_name

        with dst.open("wb") as buffer:
            shutil.copyfileobj(uploaded.file, buffer)

        saved_count += 1

    return saved_count


def build_excel_response(output_path: Path, download_name: str, temp_dir: str) -> FileResponse:
    if not output_path.exists():
        raise HTTPException(
            status_code=500,
            detail="No se pudo generar el archivo de salida"
        )

    return FileResponse(
        path=str(output_path),
        filename=download_name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        background=BackgroundTask(cleanup_temp_dir, temp_dir),
    )


@app.get(
    "/",
    summary="Estado de la API",
    tags=["Sistema"],
)
def healthcheck():
    return {
        "status": "ok",
        "message": "API activa",
        "docs": "/docs",
        "openapi": "/openapi.json",
    }


@app.get(
    "/health",
    summary="Healthcheck para monitoreo",
    tags=["Sistema"],
)
def health():
    return {"status": "ok", "service": "extractor-certificados", "version": app.version}


@app.post(
    "/extract/sinader",
    summary="Extraer PDFs SINADER a Excel",
    tags=["Extracción"],
)
async def extract_sinader(
    files: Annotated[List[UploadFile], File(..., description="Sube uno o más archivos PDF")]
):
    if not files:
        raise HTTPException(status_code=400, detail="No se subieron archivos")

    temp_dir = tempfile.mkdtemp(prefix="sinader_")
    input_dir = Path(temp_dir) / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    try:
        saved_count = save_uploaded_pdfs(files, input_dir)

        if saved_count == 0:
            raise HTTPException(status_code=400, detail="No se subieron PDFs válidos")

        output_path = Path(temp_dir) / "sinader_output.xlsx"
        process_sinader(str(input_dir), str(output_path))

        return build_excel_response(output_path, "sinader_output.xlsx", temp_dir)

    except HTTPException:
        cleanup_temp_dir(temp_dir)
        raise
    except Exception as e:
        cleanup_temp_dir(temp_dir)
        raise HTTPException(
            status_code=500,
            detail=f"Error procesando archivos SINADER: {str(e)}"
        )
    finally:
        for f in files:
            try:
                await f.close()
            except Exception:
                pass


@app.post(
    "/extract/sindrep",
    summary="Extraer PDFs SINDREP a Excel",
    tags=["Extracción"],
)
async def extract_sindrep(
    files: Annotated[List[UploadFile], File(..., description="Sube uno o más archivos PDF")]
):
    if not files:
        raise HTTPException(status_code=400, detail="No se subieron archivos")

    temp_dir = tempfile.mkdtemp(prefix="sindrep_")
    input_dir = Path(temp_dir) / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    try:
        saved_count = save_uploaded_pdfs(files, input_dir)

        if saved_count == 0:
            raise HTTPException(status_code=400, detail="No se subieron PDFs válidos")

        output_path = Path(temp_dir) / "sindrep_output.xlsx"
        process_sindrep(str(input_dir), str(output_path))

        return build_excel_response(output_path, "sindrep_output.xlsx", temp_dir)

    except HTTPException:
        cleanup_temp_dir(temp_dir)
        raise
    except Exception as e:
        cleanup_temp_dir(temp_dir)
        raise HTTPException(
            status_code=500,
            detail=f"Error procesando archivos SINDREP: {str(e)}"
        )
    finally:
        for f in files:
            try:
                await f.close()
            except Exception:
                pass
