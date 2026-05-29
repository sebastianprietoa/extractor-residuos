from __future__ import annotations

import io
import os
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Iterable

# Permite ejecutar `streamlit run app/streamlit_app.py` desde WSL o Windows
# sin depender de que el directorio padre quede agregado a sys.path.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import streamlit as st

from app.autocontrol import process_folder as process_autocontrol
from app.sinader import process_folder as process_sinader
from app.sindrep import process_folder as process_sindrep
from app.simapro import process_folder as process_simapro
from app.ui_state import clear_selection, ensure_selection_state


st.set_page_config(
    page_title="Extractor de Certificados",
    page_icon="📄",
    layout="wide",
)


PRIMARY = "#0A7B3E"
SECONDARY = "#5BBF73"
DARK = "#0F2A1D"
LIGHT_BG = "#F4FBF6"
DEFAULT_RIGHT_LOGO_URL = "https://cdn.jsdelivr.net/gh/sebastianprietoa/extractor-residuos@main/assets/logo_right.png"


def _logo_source(filename: str, env_var: str, default_url: str | None = None) -> str | None:
    env_value = os.getenv(env_var, "").strip()
    if env_value:
        return env_value
    local_path = Path("assets") / filename
    if local_path.exists():
        return str(local_path)
    return default_url


def _extract_zip_to_input(zip_bytes: bytes, input_dir: Path) -> int:
    count = 0
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            p = Path(name)
            if p.suffix.lower() != ".pdf":
                continue
            safe_name = p.name
            if not safe_name:
                continue
            dst = input_dir / f"{count + 1:03d}_{safe_name}"
            dst.write_bytes(zf.read(name))
            count += 1
    return count


def _render_header() -> None:
    st.markdown(
        """
        <style>
            .stApp { background: """ + LIGHT_BG + """; }
            .main-title {
                font-size: 6.8rem;
                line-height: 1.02;
                font-weight: 800;
                margin-bottom: 0;
                color: """ + DARK + """;
                text-align: left;
            }
            .subtitle { color: #496153; margin-top: 0.15rem; font-size: 1.05rem; }
            .box {
                border: 1px solid #d2e9d9;
                border-radius: 12px;
                padding: 1.25rem;
                background: #ffffff;
            }
            .pill {
                display: inline-block;
                padding: 0.35rem 0.7rem;
                border-radius: 999px;
                background: #e4f6ea;
                color: """ + PRIMARY + """;
                font-weight: 600;
                font-size: .86rem;
                margin-bottom: 0.5rem;
            }
            [data-testid="stSidebar"] {
                background: linear-gradient(180deg, #0f2a1d 0%, #173f2b 100%);
            }
            [data-testid="stSidebar"] * {
                color: #f4fff7 !important;
            }
            [data-testid="stFileUploader"] section {
                border: 2px dashed #9ad4aa !important;
                border-radius: 12px !important;
                background: #fcfffd;
            }
            .stButton > button {
                border: 0 !important;
                border-radius: 10px !important;
                background: linear-gradient(90deg, #0a7b3e 0%, #11a354 100%) !important;
                color: #fff !important;
                font-weight: 700 !important;
                min-height: 46px !important;
            }
            .stButton > button:hover {
                filter: brightness(1.05);
                transform: translateY(-1px);
            }
            .quick-stats {
                margin-top: 10px;
                display: grid;
                grid-template-columns: repeat(3, minmax(0,1fr));
                gap: 10px;
            }
            .quick-item {
                background: #f0fbf4;
                border: 1px solid #d7efdf;
                border-radius: 10px;
                padding: .65rem .75rem;
                color: """ + DARK + """;
                font-size: .9rem;
            }
            .quick-item b {
                display: block;
                font-size: 1rem;
                color: """ + PRIMARY + """;
            }
            .mode-btn button {
                font-size: 0.92rem !important;
                min-height: 52px !important;
                text-align: left !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<span class='pill'>Gestión sustentable</span>", unsafe_allow_html=True)
    st.markdown("<p class='main-title'>Hub Inteligente de Certificados</p>", unsafe_allow_html=True)
    st.markdown(
        "<p class='subtitle'>Centraliza SINADER, SIDREP y Autocontrol en un solo flujo.</p>",
        unsafe_allow_html=True,
    )


def _save_uploads(uploaded_files: Iterable[object], input_dir: Path) -> int:
    count = 0
    for idx, upload in enumerate(uploaded_files, start=1):
        filename = getattr(upload, "name", "")
        if not filename:
            continue
        original_name = Path(filename).name
        if original_name.startswith("~$"):
            continue
        if Path(original_name).suffix.lower() != ".pdf":
            continue
        parts = _safe_relative_parts(filename)
        if not parts:
            parts = [original_name]
        parts[-1] = f"{idx:03d}_{parts[-1]}"
        dst = input_dir.joinpath(*parts)
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(upload.getbuffer())
        count += 1
    return count


def _read_file_bytes(path: Path) -> bytes:
    with path.open("rb") as fh:
        return fh.read()


def _zip_outputs(items: list[tuple[str, bytes]]) -> bytes:
    buff = io.BytesIO()
    with zipfile.ZipFile(buff, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, data in items:
            zf.writestr(name, data)
    buff.seek(0)
    return buff.read()


def _render_preview_from_excel(path: Path, title: str) -> None:
    try:
        df = pd.read_excel(path)
    except Exception as exc:
        st.warning(f"No se pudo generar previsualización de {title}: {exc}")
        return
    st.markdown(f"#### 👀 Previsualización — {title}")
    if title.upper() == "SINADER":
        preferred = [c for c in [
            "FuentePDF", "Instalación", "Código principal", "Residuo oficial", "residuo", "Descripción Residuo",
            "Cantidad (Kg)", "Tratamiento",
            "DEFRA_Residuo", "DEFRA_Residuo_ES", "DEFRA",
        ] if c in df.columns]
    else:
        preferred = [c for c in [
            "FuentePDF", "Instalación", "N.", "Descripción Residuo", "Código principal", "Cantidad (Kg)",
            "Tratamiento", "Destino", "Transportista", "Patente",
            "DEFRA_English", "DEFRA_Español", "Clasificación DEFRA", "DEFRA",
        ] if c in df.columns]
    view = df[preferred] if preferred else df
    st.dataframe(view.head(20), use_container_width=True)


def _render_sinader_extraction_details() -> None:
    with st.expander("🧠 ¿Cómo se está extrayendo Tratamiento en SINADER?"):
        st.markdown(
            """
            1. Se extrae tabla por página (Residuo, Cantidad, Tipo Tratamiento, Destino, Transportista, Patente).  
            2. Se usa **Cantidad + `kg`** como ancla para separar texto de tratamiento y destino cuando vienen mezclados.  
            3. Se normaliza tratamiento contra catálogo de `Tratamiento_SINADER` (Nivel 3), por ejemplo:
               `Reciclaje de plásticos`, `Relleno sanitario`, `Sitio de Escombros de la Construcción`.  
            4. Si viene texto ruidoso, se limpia y se reubica en columnas correctas.
            """
        )


def _safe_relative_parts(filename: str) -> list[str]:
    normalized = filename.replace("\\", "/").strip("/")
    parts: list[str] = []
    for part in normalized.split("/"):
        if not part or part in {".", ".."}:
            continue
        if ":" in part:
            continue
        parts.append(part)
    return parts


def _save_uploads_generic(
    uploaded_files: Iterable[object],
    input_dir: Path,
    *,
    allowed_suffixes: set[str],
    preserve_structure: bool,
) -> int:
    count = 0
    for idx, upload in enumerate(uploaded_files, start=1):
        filename = getattr(upload, "name", "")
        if not filename:
            continue
        original_name = Path(filename).name
        if original_name.startswith("~$"):
            continue
        if Path(original_name).suffix.lower() not in allowed_suffixes:
            continue

        if preserve_structure:
            parts = _safe_relative_parts(filename)
            dst = input_dir.joinpath(*parts) if parts else input_dir / original_name
        else:
            dst = input_dir / f"{idx:03d}_{original_name}"

        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(upload.getbuffer())
        count += 1
    return count


def _compatible_uploaded_files(uploaded_files: Iterable[object], allowed_suffixes: set[str]) -> list[object]:
    compatible: list[object] = []
    for upload in uploaded_files:
        filename = getattr(upload, "name", "")
        if not filename:
            continue
        original_name = Path(filename).name
        if original_name.startswith("~$"):
            continue
        if Path(original_name).suffix.lower() not in allowed_suffixes:
            continue
        compatible.append(upload)
    return compatible


def _extract_zip_to_input_generic(
    zip_bytes: bytes,
    input_dir: Path,
    *,
    allowed_suffixes: set[str],
    preserve_tree: bool,
) -> int:
    count = 0
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            p = Path(name)
            if not p.name:
                continue
            if p.suffix.lower() not in allowed_suffixes:
                continue
            if p.name.startswith("~$"):
                continue

            if preserve_tree:
                parts = _safe_relative_parts(name)
                dst = input_dir.joinpath(*parts) if parts else input_dir / p.name
            else:
                dst = input_dir / f"{count + 1:03d}_{p.name}"

            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(zf.read(name))
            count += 1
    return count


def main() -> None:
    _render_header()
    st.markdown("---")

    ensure_selection_state(st.session_state)
    if "source_mode" not in st.session_state:
        st.session_state.source_mode = "AMBOS"

    with st.sidebar:
        st.header("⚙️ Configuración")
        source_labels = {
            "SINADER": "♻️ Certificado SINADER",
            "SINDREP": "🏭 Certificado SIDREP",
            "AUTOCONTROL": "🧪 Certificado Autocontrol",
            "SIMAPRO": "📊 SimaPro XLSX",
            "AMBOS": "🧩 Certificados SINADER + SIDREP",
        }
        st.caption("Selecciona función")
        for mode, label in source_labels.items():
            selected = st.session_state.source_mode == mode
            if st.button(label, key=f"btn_mode_{mode}", use_container_width=True, type="primary" if selected else "secondary"):
                st.session_state.source_mode = mode
        source = st.session_state.source_mode
        st.caption(f"Modo actual: {source.replace('SINDREP', 'SIDREP')}")
        if source == "SINADER":
            st.caption("Tratamiento se normaliza usando catálogo Nivel 3 + ancla de cantidad (kg).")
        elif source == "SIMAPRO":
            st.caption("Trabaja con XLSX/XLSM exportados desde SimaPro. Usa ZIP o carpeta local para conservar la estructura.")
        else:
            st.caption("Puedes subir PDFs o escoger carpeta desde explorador local (si ejecutas en tu PC).")
        branding_logo = _logo_source("logo_right.png", "GT_LOGO_RIGHT_URL", DEFAULT_RIGHT_LOGO_URL)
        if branding_logo:
            st.image(branding_logo, use_container_width=True)

    if source == "SIMAPRO":
        allowed_suffixes = {".xlsx", ".xlsm"}
        file_types = ["xlsx", "xlsm"]
        file_kind = "Excel"
        file_count_label = "XLSX compatibles"
        file_prompt = "📎 Arrastra una carpeta o selecciona múltiples archivos Excel"
        zip_prompt = "📦 Selecciona un ZIP con una carpeta completa de Excel"
        process_label = "Procesando Excel..."
        empty_warning = "Debes subir al menos un archivo Excel."
        incompatible_warning = "No se encontraron archivos Excel válidos en los archivos cargados."
        zip_empty_warning = "El ZIP no contiene archivos Excel válidos."
        summary_mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        allowed_suffixes = {".pdf"}
        file_types = ["pdf"]
        file_kind = "PDF"
        file_count_label = "PDFs compatibles"
        file_prompt = "📎 Arrastra una carpeta o selecciona múltiples PDFs"
        zip_prompt = "📦 Selecciona un ZIP con una carpeta completa de PDFs"
        process_label = "Procesando PDFs..."
        empty_warning = "Debes subir al menos un PDF."
        incompatible_warning = "No se encontraron PDFs válidos en los archivos cargados."
        zip_empty_warning = "El ZIP no contiene PDFs válidos."
        summary_mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    st.markdown("<div class='box'>", unsafe_allow_html=True)
    input_mode = st.radio(
        "Modo de entrada",
        options=["Subir archivos (explorador)", "Subir carpeta ZIP (explorador local)", "Seleccionar carpeta local"],
        horizontal=True,
    )

    uploads: list[object] = []
    folder_uploads: list[object] = []
    zip_upload = None
    uploader_nonce = st.session_state.uploader_nonce

    if input_mode == "Subir archivos (explorador)":
        uploads = st.file_uploader(
            file_prompt,
            type=file_types,
            accept_multiple_files=True,
            help="Abre la carpeta local en el explorador y selecciona todos los archivos compatibles.",
            key=f"file_uploader_{uploader_nonce}",
        )
    elif input_mode == "Subir carpeta ZIP (explorador local)":
        zip_upload = st.file_uploader(
            zip_prompt,
            type=["zip"],
            accept_multiple_files=False,
            help="Desde el explorador, comprime la carpeta en .zip y súbela aquí.",
            key=f"zip_uploader_{uploader_nonce}",
        )
    else:
        folder_uploads = st.file_uploader(
            f"📂 Selecciona una carpeta local con {file_kind} compatibles",
            type=file_types,
            accept_multiple_files="directory",
            help="El navegador abrirá un selector de carpetas local. Se procesarán solo los archivos compatibles.",
            key=f"folder_uploader_{uploader_nonce}",
        )
        st.caption(f"Se procesarán solo {file_count_label.lower()} dentro de la carpeta seleccionada.")
        if st.button("🧹 Limpiar carpeta", use_container_width=True, key="clear_folder_button"):
            clear_selection(st.session_state)
            st.rerun()

    if st.button("🧹 Limpiar selección", use_container_width=True, key="clear_selection_button"):
        clear_selection(st.session_state)
        st.rerun()

    run = st.button("✨ Procesar y descargar", type="primary", use_container_width=True)
    selected_uploads = list(folder_uploads or uploads or [])
    compatible_uploads = _compatible_uploaded_files(selected_uploads, allowed_suffixes)
    total_files = len(compatible_uploads)
    total_size_mb = round(sum(getattr(f, "size", 0) for f in compatible_uploads) / (1024 * 1024), 2)
    st.markdown(
        f"""
        <div class="quick-stats">
            <div class="quick-item"><b>{source}</b>Modo seleccionado</div>
            <div class="quick-item"><b>{total_files}</b>{file_count_label}</div>
            <div class="quick-item"><b>{total_size_mb} MB</b>Peso total</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    if not run:
        return

    if input_mode == "Subir carpeta ZIP (explorador local)":
        if not zip_upload:
            st.warning(f"Debes subir un archivo ZIP con {file_kind}s.")
            return
        with st.spinner("Procesando carpeta ZIP..."):
            with tempfile.TemporaryDirectory(prefix="streamlit_extract_folder_") as temp_dir:
                tmp = Path(temp_dir)
                input_dir = tmp / "input"
                input_dir.mkdir(parents=True, exist_ok=True)
                total = _extract_zip_to_input_generic(
                    zip_upload.getvalue(),
                    input_dir,
                    allowed_suffixes=allowed_suffixes,
                    preserve_tree=True,
                )
                if total == 0:
                    st.warning(zip_empty_warning)
                    return
                if source == "SIMAPRO":
                    output = tmp / "simapro_output.xlsx"
                    process_simapro(str(input_dir), str(output))
                    df = _render_preview_from_excel(output, "SIMAPRO")
                    if df is not None and df.empty:
                        st.warning("Se detectaron archivos Excel, pero no se encontraron registros compatibles en la hoja procesada.")
                    st.success(f"Proceso SIMAPRO completado desde carpeta ZIP. {total} archivos compatibles procesados.")
                    st.download_button(
                        "Descargar resultado",
                        data=output.read_bytes(),
                        file_name=output.name,
                        mime=summary_mime,
                        use_container_width=True,
                    )
                    return
                if source == "SINADER":
                    output = tmp / "sinader_output.xlsx"
                    process_sinader(str(input_dir), str(output))
                    _render_sinader_extraction_details()
                    _render_preview_from_excel(output, "SINADER")
                    data = output.read_bytes()
                    filename = output.name
                    mime = summary_mime
                elif source == "SINDREP":
                    output = tmp / "sindrep_output.xlsx"
                    process_sindrep(str(input_dir), str(output))
                    _render_preview_from_excel(output, "SIDREP")
                    data = output.read_bytes()
                    filename = output.name
                    mime = summary_mime
                elif source == "AUTOCONTROL":
                    output = tmp / "autocontrol_output.xlsx"
                    process_autocontrol(str(input_dir), str(output))
                    _render_preview_from_excel(output, "AUTOCONTROL")
                    data = output.read_bytes()
                    filename = output.name
                    mime = summary_mime
                else:
                    out_sinader = tmp / "sinader_output.xlsx"
                    out_sindrep = tmp / "sindrep_output.xlsx"
                    process_sinader(str(input_dir), str(out_sinader))
                    process_sindrep(str(input_dir), str(out_sindrep))
                    _render_sinader_extraction_details()
                    _render_preview_from_excel(out_sinader, "SINADER")
                    _render_preview_from_excel(out_sindrep, "SIDREP")
                    data = _zip_outputs([
                        (out_sinader.name, out_sinader.read_bytes()),
                        (out_sindrep.name, out_sindrep.read_bytes()),
                    ])
                    filename = "resultados_extraccion.zip"
                    mime = "application/zip"
        st.success("Proceso completado desde carpeta ZIP.")
        st.download_button("Descargar resultado", data=data, file_name=filename, mime=mime, use_container_width=True)
        return

    if not selected_uploads:
        st.warning(empty_warning)
        return

    if not compatible_uploads:
        st.error(incompatible_warning)
        return

    with st.spinner(process_label):
        with tempfile.TemporaryDirectory(prefix="streamlit_extract_") as temp_dir:
            tmp = Path(temp_dir)
            input_dir = tmp / "input"
            input_dir.mkdir(parents=True, exist_ok=True)

            if source == "SIMAPRO":
                total = _save_uploads_generic(
                    compatible_uploads,
                    input_dir,
                    allowed_suffixes=allowed_suffixes,
                    preserve_structure=input_mode == "Seleccionar carpeta local",
                )
            else:
                total = _save_uploads(compatible_uploads, input_dir)

            if total == 0:
                st.error(incompatible_warning)
                return

            if source == "SIMAPRO":
                output = tmp / "simapro_output.xlsx"
                process_simapro(str(input_dir), str(output))
                df = _render_preview_from_excel(output, "SIMAPRO")
                if df is not None and df.empty:
                    st.warning("Se detectaron archivos Excel, pero no se encontraron registros compatibles en la hoja procesada.")
                st.success(f"Proceso SIMAPRO completado. {total} archivos compatibles procesados.")
                st.download_button(
                    label="Descargar Excel SIMAPRO",
                    data=output.read_bytes(),
                    file_name=output.name,
                    mime=summary_mime,
                    use_container_width=True,
                )
                return

            if source == "AUTOCONTROL":
                output = tmp / "autocontrol_output.xlsx"
                process_autocontrol(str(input_dir), str(output))
                _render_preview_from_excel(output, "AUTOCONTROL")
                data = _read_file_bytes(output)
                st.success(f"Proceso AUTOCONTROL completado. PDFs procesados: {total}")
                st.download_button(
                    label="Descargar Excel AUTOCONTROL",
                    data=data,
                    file_name=output.name,
                    mime=summary_mime,
                    use_container_width=True,
                )
                return

            outputs: list[tuple[str, bytes]] = []
            if source in {"SINADER", "AMBOS"}:
                out_sinader = tmp / "sinader_output.xlsx"
                process_sinader(str(input_dir), str(out_sinader))
                _render_sinader_extraction_details()
                _render_preview_from_excel(out_sinader, "SINADER")
                outputs.append((out_sinader.name, _read_file_bytes(out_sinader)))

            if source in {"SINDREP", "AMBOS"}:
                out_sindrep = tmp / "sindrep_output.xlsx"
                process_sindrep(str(input_dir), str(out_sindrep))
                _render_preview_from_excel(out_sindrep, "SIDREP")
                outputs.append((out_sindrep.name, _read_file_bytes(out_sindrep)))

    st.success(f"Proceso completado. PDFs procesados: {total}")
    st.balloons()

    if source == "AMBOS":
        zip_bytes = _zip_outputs(outputs)
        st.download_button(
            label="Descargar resultados (ZIP)",
            data=zip_bytes,
            file_name="resultados_extraccion.zip",
            mime="application/zip",
            use_container_width=True,
        )
        return

    name, data = outputs[0]
    st.download_button(
        label="Descargar Excel",
        data=data,
        file_name=name,
        mime=summary_mime,
        use_container_width=True,
    )


if __name__ == "__main__":
    main()
