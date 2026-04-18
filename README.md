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
- `GET /ui` → interfaz web para seleccionar carpeta con PDFs y descargar Excel
- `GET /health` → healthcheck para monitoreo (Railway/Uptime checks)
- `POST /extract/sinader` → recibe PDFs y devuelve un Excel
- `POST /extract/sindrep` → recibe PDFs y devuelve un Excel

## Uso por carpeta (CLI)

Si prefieres ejecutar la extracción localmente desde una carpeta de PDFs (sin subir archivos por API):

```bash
python -m app.cli
```

La herramienta te pedirá:
- Ruta de la carpeta que contiene los PDFs (búsqueda recursiva)

Por defecto procesa **ambos tipos** y genera:
- `sinader_output_YYYYMMDD_HHMMSS.xlsx`
- `sindrep_output_YYYYMMDD_HHMMSS.xlsx`

También puedes usar parámetros:

```bash
python -m app.cli --source sinader --input-dir ./mis_pdfs --output ./resultado.xlsx
# o:
python -m app.cli --source ambos --input-dir ./mis_pdfs
```

## Interfaz amigable con Streamlit

Para una experiencia más simple de usuario final (con opción de logos):

```bash
streamlit run app/streamlit_app.py
```

Luego abre la URL que te muestre Streamlit (normalmente `http://localhost:8501`).

En la barra lateral puedes elegir:
- `SINADER`
- `SINDREP`
- `AMBOS`
- `AUTOCONTROL` (subida de PDFs desde carpeta local usando el explorador)

La UI permite dos modos de entrada:
- `Subir PDFs (explorador)`
- `Escoger carpeta (explorador local)` (abre selector de carpeta cuando ejecutas Streamlit en tu PC)

### Logos de empresa

Si quieres mostrar tus logos en la cabecera, agrega archivos en:
- `assets/logo_right.png`

También puedes configurarlo por variable de entorno:
- `GT_LOGO_RIGHT_URL`

Si `GT_LOGO_RIGHT_URL` no está definida, la UI usa por defecto:
- `https://cdn.jsdelivr.net/gh/sebastianprietoa/extractor-residuos@main/assets/logo_right.png`

## Deploy en Railway

1. Sube este proyecto a GitHub.
2. En Railway crea un proyecto desde ese repositorio.
3. Configura variable de entorno `APP_MODE` según lo que quieras publicar:
   - `fastapi` (default): API + docs (`/docs`, `/ui`)
   - `streamlit`: interfaz Streamlit en la raíz del dominio
4. Verifica que el Start Command sea:

```bash
bash -lc 'if [ "$APP_MODE" = "streamlit" ]; then /app/.venv/bin/python -m streamlit run app/streamlit_app.py --server.address 0.0.0.0 --server.port ${PORT:-8501} --server.headless true; else /app/.venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}; fi'
```

## Notas

- Los Excel generados se crean temporalmente por solicitud.
- Se aceptan content-types comunes de PDF (`application/pdf`, `application/x-pdf`, `application/octet-stream`) para evitar rechazos por variaciones del cliente.
- Si luego quieres interfaz web, se puede agregar HTML/Jinja o React encima de esta API.
- Para catálogo SINADER en Excel se prioriza la hoja `LER_completo_842` (si existe) y columnas como `Código LER` + `Entry official name (EN)`/`Descripción`; también filtra `¿Declarable en SINADER? = Sí` cuando esa columna está presente.
- Para mapear `Tratamiento` a nombres `DEFRA` se usa la hoja `Tratamiento_SINADER`; si no existe, se aplica un mapeo base (`Reutilización→Re-use`, `Reciclaje→Open-loop`, `Combustión→Combustion`, `Vertedero→Landfill`, `Anaerobic digestion→Anaerobic digestion`).
