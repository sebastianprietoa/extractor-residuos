from pathlib import Path
import shutil
import tempfile

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse

from app.sinader import process_folder as process_sinader
from app.sindrep import process_folder as process_sindrep

app = FastAPI(title="Extractor de Certificados", version="1.0.0")


@app.get("/")
def healthcheck():
    return {"status": "ok", "message": "API activa"}


@app.post("/extract/sinader")
async def extract_sinader(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No se subieron archivos")

    temp_dir = tempfile.mkdtemp(prefix="sinader_")
    input_dir = Path(temp_dir) / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    for f in files:
        dst = input_dir / f.filename
        with dst.open("wb") as buffer:
            shutil.copyfileobj(f.file, buffer)

    output_path = Path(temp_dir) / "sinader_output.xlsx"
    process_sinader(str(input_dir), str(output_path))

    return FileResponse(
        path=str(output_path),
        filename="sinader_output.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.post("/extract/sindrep")
async def extract_sindrep(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No se subieron archivos")

    temp_dir = tempfile.mkdtemp(prefix="sindrep_")
    input_dir = Path(temp_dir) / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    for f in files:
        dst = input_dir / f.filename
        with dst.open("wb") as buffer:
            shutil.copyfileobj(f.file, buffer)

    output_path = Path(temp_dir) / "sindrep_output.xlsx"
    process_sindrep(str(input_dir), str(output_path))

    return FileResponse(
        path=str(output_path),
        filename="sindrep_output.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
