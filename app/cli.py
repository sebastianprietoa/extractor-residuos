from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Callable

from app.sinader import process_folder as process_sinader
from app.sindrep import process_folder as process_sindrep


ExtractorFn = Callable[[str, str], object]


def _ask_non_empty(prompt: str) -> str:
    while True:
        value = input(prompt).strip()
        if value:
            return value
        print("⚠️  Debes ingresar un valor.")


def _resolve_extractor(source: str) -> ExtractorFn:
    return process_sinader if source == "sinader" else process_sindrep


def _default_output(source: str) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path.cwd() / f"{source}_output_{stamp}.xlsx"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Procesa una carpeta con PDFs y genera un Excel de salida.",
    )
    parser.add_argument(
        "--source",
        choices=["sinader", "sindrep", "ambos"],
        default="ambos",
        help="Tipo de certificado a procesar. Por defecto: ambos.",
    )
    parser.add_argument(
        "--input-dir",
        help="Carpeta que contiene PDFs (se busca de forma recursiva).",
    )
    parser.add_argument(
        "--output",
        help="Ruta del archivo Excel de salida.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    input_dir_raw = args.input_dir or _ask_non_empty("Ruta de carpeta con PDFs: ")
    input_dir = Path(input_dir_raw).expanduser().resolve()
    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"La carpeta no existe o no es válida: {input_dir}")

    source = args.source

    if source == "ambos":
        sinader_output = _default_output("sinader")
        sindrep_output = _default_output("sindrep")
        process_sinader(str(input_dir), str(sinader_output))
        process_sindrep(str(input_dir), str(sindrep_output))
        print("✅ Proceso finalizado.")
        print(f"   - SINADER: {sinader_output}")
        print(f"   - SINDREP: {sindrep_output}")
        return

    output = Path(args.output).expanduser().resolve() if args.output else _default_output(source)
    extractor = _resolve_extractor(source)
    extractor(str(input_dir), str(output))
    print(f"✅ Proceso finalizado. Excel generado en:\n{output}")


if __name__ == "__main__":
    main()
