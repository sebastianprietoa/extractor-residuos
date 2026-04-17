from __future__ import annotations

import io
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Iterable

import streamlit as st

from app.sinader import process_folder as process_sinader
from app.sindrep import process_folder as process_sindrep


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


def _render_header() -> None:
    st.markdown(
        """
        <style>
            .stApp { background: """ + LIGHT_BG + """; }
            .main-title { font-size: 2.2rem; font-weight: 800; margin-bottom: 0; color: """ + DARK + """; }
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
        </style>
        """,
        unsafe_allow_html=True,
    )

    _, center, _ = st.columns([1, 3, 1])
    with center:
        st.markdown("<span class='pill'>Gestión sustentable</span>", unsafe_allow_html=True)
        st.markdown("<p class='main-title'>Extractor SINADER / SINDREP</p>", unsafe_allow_html=True)
        st.markdown(
            "<p class='subtitle'>Sube tus PDFs, procesa con un clic y descarga tu Excel listo para usar.</p>",
            unsafe_allow_html=True,
        )


def _save_uploads(uploaded_files: Iterable[object], input_dir: Path) -> int:
    count = 0
    for idx, upload in enumerate(uploaded_files, start=1):
        filename = Path(upload.name).name
        if Path(filename).suffix.lower() != ".pdf":
            continue
        dst = input_dir / f"{idx:03d}_{filename}"
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


def main() -> None:
    _render_header()
    st.markdown("---")

    with st.sidebar:
        st.header("⚙️ Configuración")
        source = st.selectbox(
            "Tipo de extracción",
            options=["SINADER", "SINDREP", "AMBOS"],
            index=2,
        )
        st.caption("Tip: para ambos tipos se descarga un ZIP con dos Excel.")
        branding_logo = _logo_source("logo_right.png", "GT_LOGO_RIGHT_URL", DEFAULT_RIGHT_LOGO_URL)
        if branding_logo:
            st.image(branding_logo, use_container_width=True)

    st.markdown("<div class='box'>", unsafe_allow_html=True)
    uploads = st.file_uploader(
        "📎 Arrastra una carpeta o selecciona múltiples PDFs",
        type=["pdf"],
        accept_multiple_files=True,
        help="Puedes seleccionar múltiples PDFs. La app procesa todo en una sola ejecución.",
    )
    run = st.button("✨ Procesar y descargar", type="primary", use_container_width=True)
    total_files = len(uploads or [])
    total_size_mb = round(sum(getattr(f, "size", 0) for f in (uploads or [])) / (1024 * 1024), 2)
    st.markdown(
        f"""
        <div class="quick-stats">
            <div class="quick-item"><b>{source}</b>Modo seleccionado</div>
            <div class="quick-item"><b>{total_files}</b>PDFs cargados</div>
            <div class="quick-item"><b>{total_size_mb} MB</b>Peso total</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    if not run:
        return

    if not uploads:
        st.warning("Debes subir al menos un PDF.")
        return

    with st.spinner("Procesando PDFs..."):
        with tempfile.TemporaryDirectory(prefix="streamlit_extract_") as temp_dir:
            tmp = Path(temp_dir)
            input_dir = tmp / "input"
            input_dir.mkdir(parents=True, exist_ok=True)

            total = _save_uploads(uploads, input_dir)
            if total == 0:
                st.error("No se encontraron PDFs válidos en los archivos cargados.")
                return

            outputs: list[tuple[str, bytes]] = []
            if source in {"SINADER", "AMBOS"}:
                out_sinader = tmp / "sinader_output.xlsx"
                process_sinader(str(input_dir), str(out_sinader))
                outputs.append((out_sinader.name, _read_file_bytes(out_sinader)))

            if source in {"SINDREP", "AMBOS"}:
                out_sindrep = tmp / "sindrep_output.xlsx"
                process_sindrep(str(input_dir), str(out_sindrep))
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
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )


if __name__ == "__main__":
    main()
