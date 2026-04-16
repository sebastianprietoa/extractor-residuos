from __future__ import annotations

import io
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


def _render_header() -> None:
    st.markdown(
        """
        <style>
            .main-title { font-size: 2rem; font-weight: 700; margin-bottom: 0; }
            .subtitle { color: #64748b; margin-top: 0.15rem; }
            .box {
                border: 1px solid #e2e8f0;
                border-radius: 12px;
                padding: 1rem;
                background: #ffffff;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    left, center, right = st.columns([1, 3, 1])
    with left:
        if Path("assets/logo_left.png").exists():
            st.image("assets/logo_left.png", use_container_width=True)
    with center:
        st.markdown("<p class='main-title'>Extractor SINADER / SINDREP</p>", unsafe_allow_html=True)
        st.markdown(
            "<p class='subtitle'>Sube tus PDFs y descarga el Excel automáticamente.</p>",
            unsafe_allow_html=True,
        )
    with right:
        if Path("assets/logo_right.png").exists():
            st.image("assets/logo_right.png", use_container_width=True)


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
        st.header("Configuración")
        source = st.selectbox(
            "Tipo de extracción",
            options=["SINADER", "SINDREP", "AMBOS"],
            index=2,
        )
        st.caption("Tip: para ambos tipos se descarga un ZIP con dos Excel.")

    st.markdown("<div class='box'>", unsafe_allow_html=True)
    uploads = st.file_uploader(
        "Arrastra una carpeta o selecciona múltiples PDFs",
        type=["pdf"],
        accept_multiple_files=True,
        help="Puedes seleccionar múltiples PDFs. La app procesa todo en una sola ejecución.",
    )
    run = st.button("Procesar y descargar", type="primary", use_container_width=True)
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
