# Certificados Extractor

Aplicacion web para extraer informacion desde certificados PDF de SINADER, SINDREP y Autocontrol, y desde archivos XLSX exportados por SimaPro.

## Estructura

- `app/main.py`: API FastAPI
- `app/streamlit_app.py`: interfaz Streamlit
- `app/sinader.py`: extractor SINADER
- `app/sindrep.py`: extractor SINDREP
- `app/autocontrol.py`: extractor Autocontrol
- `app/simapro.py`: extractor SimaPro
- `app/cli.py`: ejecutor por linea de comandos
- `requirements.txt`: dependencias
- `Procfile`: comando de inicio para Railway

## Uso local

```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
uvicorn app.main:app --reload
```

La API queda disponible en:

- `http://127.0.0.1:8000`
- Documentacion interactiva: `http://127.0.0.1:8000/docs`

## Endpoints

- `GET /` -> estado de la API
- `GET /ui` -> interfaz web para subir carpetas o archivos
- `GET /health` -> healthcheck
- `POST /extract/sinader` -> recibe PDFs y devuelve un Excel
- `POST /extract/sindrep` -> recibe PDFs y devuelve un Excel
- `POST /extract/simapro` -> recibe XLSX y devuelve un Excel consolidado

## Uso por consola

```bash
python -m app.cli --source simapro --input-dir ./Recursos
python -m app.cli --source sinader --input-dir ./mis_pdfs --output ./resultado.xlsx
python -m app.cli --source ambos --input-dir ./mis_pdfs
```

## Interfaz Streamlit

```bash
streamlit run app/streamlit_app.py
```

En la barra lateral puedes elegir:

- `SINADER`
- `SINDREP`
- `AUTOCONTROL`
- `SIMAPRO`
- `AMBOS`

Para SimaPro, la estructura de carpetas se conserva mejor si subes un ZIP o si el navegador entrega la ruta relativa al seleccionar una carpeta local.
La UI también permite elegir una carpeta local y procesar de forma recursiva todos los archivos compatibles con el modo seleccionado.

## Notas

- Los Excel generados se crean temporalmente por solicitud.
- Para SimaPro se conservan carpetas y subcarpetas cuando la ruta llega completa al servidor.
- Si luego quieres exportar tambien CSV o JSON desde la app principal, se puede agregar sin tocar el extractor base.
