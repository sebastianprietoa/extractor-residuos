from pathlib import Path
import shutil
import tempfile
from typing import Annotated, List
from uuid import uuid4

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
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

WEB_UI_HTML = """
<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Extractor de Certificados</title>
    <style>
      body { font-family: Arial, sans-serif; max-width: 860px; margin: 40px auto; padding: 0 16px; }
      h1 { margin-bottom: 8px; }
      .box { border: 1px solid #ddd; border-radius: 10px; padding: 16px; margin-top: 16px; }
      label { font-weight: 600; display: block; margin: 10px 0 6px; }
      select, input, button { font-size: 15px; }
      input[type="file"] { width: 100%; }
      button { margin-top: 14px; padding: 10px 16px; cursor: pointer; }
      .muted { color: #666; font-size: 14px; }
      #status { margin-top: 12px; white-space: pre-wrap; }
    </style>
  </head>
  <body>
    <h1>Extractor SINADER / SINDREP</h1>
    <p class="muted">Selecciona una carpeta local con PDFs. El sistema sube los archivos y devuelve un Excel.</p>

    <div class="box">
      <label for="source">Tipo de extracción</label>
      <select id="source">
        <option value="sinader">SINADER</option>
        <option value="sindrep">SINDREP</option>
      </select>

      <label for="folder">Carpeta con PDFs</label>
      <input id="folder" type="file" webkitdirectory directory multiple accept=".pdf,application/pdf" />
      <p class="muted">Nota: por seguridad del navegador, se selecciona la carpeta y se suben los archivos PDF, no la ruta del disco.</p>

      <button id="run">Procesar y descargar Excel</button>
      <div id="status"></div>
    </div>

    <script>
      const statusEl = document.getElementById("status");
      document.getElementById("run").addEventListener("click", async () => {
        const source = document.getElementById("source").value;
        const files = Array.from(document.getElementById("folder").files || []).filter(f => f.name.toLowerCase().endsWith(".pdf"));

        if (!files.length) {
          statusEl.textContent = "⚠️ Debes seleccionar una carpeta que contenga PDFs.";
          return;
        }

        statusEl.textContent = `Procesando ${files.length} PDF(s)...`;
        const form = new FormData();
        files.forEach(f => form.append("files", f));

        try {
          const resp = await fetch(`/extract/${source}`, { method: "POST", body: form });
          if (!resp.ok) {
            const detail = await resp.text();
            statusEl.textContent = `❌ Error (${resp.status}): ${detail}`;
            return;
          }
          const blob = await resp.blob();
          const url = window.URL.createObjectURL(blob);
          const a = document.createElement("a");
          a.href = url;
          a.download = `${source}_output.xlsx`;
          document.body.appendChild(a);
          a.click();
          a.remove();
          window.URL.revokeObjectURL(url);
          statusEl.textContent = "✅ Excel generado y descargado.";
        } catch (err) {
          statusEl.textContent = `❌ Error de red: ${err}`;
        }
      });
    </script>
  </body>
</html>
"""

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
        "ui": "/ui",
        "docs": "/docs",
        "openapi": "/openapi.json",
    }


@app.get(
    "/ui",
    summary="Interfaz web para subir carpeta de PDFs",
    tags=["Sistema"],
    response_class=HTMLResponse,
)
def ui():
    return HTMLResponse(WEB_UI_HTML)


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
    response_class=FileResponse,
    responses={
        200: {
            "description": "Excel de salida",
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
        }
    },
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
    response_class=FileResponse,
    responses={
        200: {
            "description": "Excel de salida",
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
        }
    },
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
