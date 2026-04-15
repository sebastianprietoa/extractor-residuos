# Certificados Extractor

Aplicación web para extraer información desde certificados PDF de SINADER y SINDREP.

## Estructura

- `app/main.py`: API FastAPI
- `app/sinader.py`: extractor SINADER
- `app/sindrep.py`: extractor SINDREP
- `requirements.txt`: dependencias
- `Procfile`: comando de inicio para Railway

## Uso local

```bash
python -m venv .venv
source .venv/bin/activate  # en Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

La API quedará disponible en:
- `http://127.0.0.1:8000`
- Documentación interactiva: `http://127.0.0.1:8000/docs`

## Endpoints

- `GET /` → estado de la API + accesos rápidos a `/docs` y `/openapi.json`
- `GET /health` → healthcheck para monitoreo (Railway/Uptime checks)
- `POST /extract/sinader` → recibe PDFs y devuelve un Excel
- `POST /extract/sindrep` → recibe PDFs y devuelve un Excel

## Uso por carpeta (CLI)

Si prefieres ejecutar la extracción localmente desde una carpeta de PDFs (sin subir archivos por API):

```bash
python -m app.cli
```

La herramienta te pedirá:
- Tipo de certificado (`sinader` o `sindrep`)
- Ruta de la carpeta que contiene los PDFs (búsqueda recursiva)

También puedes usar parámetros:

```bash
python -m app.cli --source sinader --input-dir ./mis_pdfs --output ./resultado.xlsx
```

## Deploy en Railway

1. Sube este proyecto a GitHub.
2. En Railway crea un proyecto desde ese repositorio.
3. Verifica que el Start Command sea:

```bash
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

## Notas

- Los Excel generados se crean temporalmente por solicitud.
- Se aceptan content-types comunes de PDF (`application/pdf`, `application/x-pdf`, `application/octet-stream`) para evitar rechazos por variaciones del cliente.
- Si luego quieres interfaz web, se puede agregar HTML/Jinja o React encima de esta API.
