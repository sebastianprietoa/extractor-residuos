import re
import unicodedata
import logging
import os
import glob
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from difflib import SequenceMatcher

import pandas as pd
import pdfplumber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

STREAM_SETTINGS = {
    "vertical_strategy": "text",
    "horizontal_strategy": "text",
    "snap_tolerance": 3,
    "join_tolerance": 3,
    "edge_min_length": 3,
    "min_words_vertical": 1,
    "min_words_horizontal": 1,
    "intersection_tolerance": 3,
    "text_tolerance": 3,
}

MASTER_RESIDUOS: Dict[str, List[str]] = {
    "02 01 99": ["Residuos no especificados en otra categoría"],
    "02 02 04": ["Lodos del tratamiento in situ de efluentes"],
    "10 01 01": ["Cenizas del hogar, escorias y polvo de caldera (excepto el polvo de caldera especificado en el código 10 01 04)"],
    "15 01 01": ["Envases de papel y cartón"],
    "15 01 02": ["Envases de plástico"],
    "15 01 04": ["Envases metálicos"],
    "19 08 05": ["Lodos del tratamiento de aguas residuales urbanas"],
    "20 01 99": ["Otras fracciones no especificadas en otra categoría"],
    "21 04 04": ["Residuos de plásticos (HDPE, PEE, PETE, PVC) excepto planzas, boyas, flotadores, redes y cabos."],
}

DEFAULT_CATALOG_PATH = Path("assets/sinader_codigos.xlsx")
PREFERRED_CATALOG_SHEETS = ("LER_completo_842",)
TREATMENT_CATALOG_SHEET = "Tratamiento_SINADER"
DEFAULT_TREATMENT_DEFRA_MAP = {
    "reutilizacion": "Re-use",
    "reciclaje": "Open-loop",
    "combustion": "Combustion",
    "vertedero": "Landfill",
    "anaerobic digestion": "Anaerobic digestion",
}
DEFAULT_TREATMENT_TRAINING_GLOB = "assets/*output*.xlsx"
KNOWN_DESTINATIONS = [
    "ECOPRIAL",
    "ECOFIBRAS SUCURSAL PUERTO MONTT",
    "PLASTICOS DEL SUR SPA",
    "CONSORCIO COLLIPULLI",
    "ESCOMBRERA TRESOL",
    "CENTRO CRUCERO",
    "PLANTA DE TRATAMIENTO DE AGUAS SERVIDAS DE CASTRO",
    "RELLENO SANITARIO LOS ANGELES",
    "PLANTA DE TRATAMIENTO DE RESIDUOS DOMICILIARIOS LAUTARO",
    "SALMONOIL S.A.",
    "LOS GLACIARES",
    "PESQUERA LANDES ISLA ROCUANT",
    "PESQUERA LA PORTADA",
    "REPLACAR",
    "PLANTA RILESUR",
    "ESTACIÓN DE TRANSFERENCIA",
    "ESTACION DE TRANSFERENCIA",
    "ECOFIBRAS SUCURSAL CORONEL",
    "ECOBIO",
    "CANCHA COMPOSTAJE LOS REBALSES DEL SUR",
    "CANCHA LOS REBALSES DEL SUR",
]
DESTINATION_NOISE_FRAGMENTS = [
    "en otra categoría",
    "planzas, boyas, flotadores, redes y cabos",
    "cenizas del hogar",
    "lodos del tratamiento",
    "del tratamiento in situ de efluentes",
    "cartón y productos de papel",
]
TREATMENT_NOISE_FRAGMENTS = [
    "en otra categoría",
    "especificadas en otra categoría",
    "planzas, boyas, flotadores, redes y cabos",
    "cenizas del hogar",
    "lodos del tratamiento in situ de efluentes",
]
STRONG_TREATMENT_CATALOG = [
    "Reciclaje de plásticos",
    "Reciclaje de metales",
    "Reciclaje de papel, cartón y productos de papel",
    "Relleno sanitario",
    "Monorelleno",
    "Degradación Anaeróbica",
    "Compostaje",
    "Recepción de Lodos en PTAS",
    "Sitio de Escombros de la Construcción",
    "Pretratamiento",
    "Pretratamiento de plásticos",
    "Reciclaje de residuos hidrobiológicos para consumo animal",
    "Residuos municipales asimilables a domiciliarios",
    "Disposición final",
]
KNOWN_SINADER_CODES = {
    "15 01 01", "15 01 02", "15 01 04", "20 01 99", "19 08 05", "10 01 01", "21 04 04", "02 02 04",
    "02 01 99", "02 01 02", "02 02 02", "02 02 03", "20 01 39", "15 01 06", "21 07 09", "21 07 01",
}


def _strip_accents(s: str) -> str:
    if s is None:
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")


def _norm(s: str) -> str:
    s = _strip_accents(s or "")
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _clean_cell(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _normalize_code(code: Optional[str]) -> str:
    raw = _clean_cell(code)
    if not raw:
        return ""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 6:
        return f"{digits[0:2]} {digits[2:4]} {digits[4:6]}"
    if len(digits) > 6:
        return f"{digits[0:2]} {digits[2:4]} {digits[4:6]}"
    return raw


def _cell_join_multiline(x: Optional[str]) -> str:
    if x is None:
        return ""
    x = str(x).replace("\r", "\n").replace("\u00a0", " ").replace("\n", " ")
    x = re.sub(r"\s+", " ", x).strip()
    return x


def _parse_spanish_month(text_norm: str) -> Optional[int]:
    month_map = {
        "ene": 1, "enero": 1,
        "feb": 2, "febrero": 2,
        "mar": 3, "marzo": 3,
        "abr": 4, "abril": 4,
        "may": 5, "mayo": 5,
        "jun": 6, "junio": 6,
        "jul": 7, "julio": 7,
        "ago": 8, "agosto": 8,
        "sep": 9, "set": 9, "septiembre": 9,
        "oct": 10, "octubre": 10,
        "nov": 11, "noviembre": 11,
        "dic": 12, "diciembre": 12,
    }
    for k, v in month_map.items():
        if re.search(rf"\b{k}\b", text_norm):
            return v
    return None


def _to_float_kg(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    s = s.replace(" ", "")
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    s = re.sub(r"[^0-9\.\-]", "", s)
    if s in ("", ".", "-", "-.", ".-"):
        return None
    try:
        return float(s)
    except Exception:
        return None


def is_sinader_pdf(full_text: str) -> bool:
    t = _norm(full_text)
    keys = ["sinader", "retc", "declaracion mensual", "residuos no peligrosos", "comprobante"]
    return sum(1 for k in keys if k in t) >= 2


def extract_key_value_lines(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    lines = [l.strip() for l in (text or "").splitlines() if l.strip()]
    for ln in lines:
        m = re.match(r"^([A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9\.\-/\s]{2,80})\s*:\s*(.+)$", ln)
        if m:
            out[_norm(_clean_cell(m.group(1)))] = _clean_cell(m.group(2))
    return out


def parse_period_from_filename(filename: str) -> Optional[str]:
    t = _norm(Path(filename).stem)
    ym = re.search(r"\b(20\d{2})\b", t)
    year = int(ym.group(1)) if ym else None
    month = _parse_spanish_month(t)
    return f"{month:02d}/{year}" if year and month else None


def extract_period_from_text(full_text: str) -> Optional[str]:
    t = _strip_accents(full_text or "")
    m = re.search(r"Periodo\s+declarado\s*:\s*(\d{2}\s*/\s*\d{4})", t, flags=re.IGNORECASE)
    if m:
        return re.sub(r"\s+", "", m.group(1))
    m2 = re.search(r"Periodo\s*:\s*(\d{4}\s*[-/]\s*\d{2}|\d{2}\s*[-/]\s*\d{4})", t, flags=re.IGNORECASE)
    if m2:
        raw = re.sub(r"\s+", "", m2.group(1))
        if re.match(r"^\d{4}[-/]\d{2}$", raw):
            y, mm = re.split(r"[-/]", raw)
            return f"{mm}/{y}"
        if re.match(r"^\d{2}[-/]\d{4}$", raw):
            mm, y = re.split(r"[-/]", raw)
            return f"{mm}/{y}"
    return None


def sinader_has_no_movements(full_text: str) -> bool:
    t = _norm(full_text)
    patterns = ["periodo sin movimientos", "período sin movimientos", "sin movimientos", "no registra movimientos"]
    return any(p in t for p in patterns)


def extract_sinader_metadata(full_text: str, filename: str) -> Dict[str, str]:
    kv = extract_key_value_lines(full_text)

    def get_any(*keys: str) -> str:
        for k in keys:
            kn = _norm(k)
            if kn in kv:
                return kv[kn]
        for kk, vv in kv.items():
            for k in keys:
                if _norm(k) in kk:
                    return vv
        return ""

    meta = {
        "FuentePDF": Path(filename).name,
        "Folio": get_any("Folio"),
        "Establecimiento": get_any("Establecimiento"),
        "Razón social": get_any("Razón social", "Razon social"),
        "RUT Titular": get_any("RUT Titular", "Rut titular", "RUT"),
        "Realizado por": get_any("Realizado por"),
        "Tipo": get_any("Tipo"),
        "Estado": get_any("Estado"),
        "Código identificador": get_any("Código identificador", "Codigo identificador"),
        "Región": get_any("Región", "Region"),
        "Comuna": get_any("Comuna"),
    }
    meta["Periodo declarado"] = (
        extract_period_from_text(full_text)
        or get_any("Periodo declarado", "Periodo")
        or parse_period_from_filename(filename)
        or ""
    )
    return meta


def _looks_like_sinader_table(table: List[List[str]]) -> bool:
    if not table or not table[0]:
        return False
    header = " ".join(_norm(_clean_cell(c)) for c in table[0] if c is not None)
    return ("residuo" in header) and ("cantidad" in header) and ("destino" in header)


def _split_code_and_desc(residuo_cell: str) -> Tuple[str, str]:
    s = _cell_join_multiline(residuo_cell)
    s = re.sub(r"\s*\|\s*", " | ", s)
    if " | " in s:
        left, right = s.split(" | ", 1)
        return _clean_cell(left), _clean_cell(right)
    m = re.match(r"^\s*(\d{2}\s+\d{2}\s+\d{2})\s+(.*)$", s)
    if m:
        return _clean_cell(m.group(1)), _clean_cell(m.group(2))
    return "", s


def _extract_table_text_block(full_text: str) -> str:
    text = full_text.replace("\r", "\n").replace("\u00a0", " ")
    start_match = re.search(r"Residuo.*Cantidad.*Destino", text, flags=re.IGNORECASE | re.DOTALL)
    if not start_match:
        return text
    block = text[start_match.start():]
    end_match = re.search(r"La\s+integridad\s+y\s+veracidad", block, flags=re.IGNORECASE)
    if end_match:
        block = block[:end_match.start()]
    return block


def _reconstruct_row_blocks_from_lines(lines: List[str]) -> List[str]:
    blocks: List[str] = []
    current: List[str] = []
    ler_start = re.compile(r"^\s*\d{2}\s+\d{2}\s+\d{2}\s*\|")
    header_noise = re.compile(r"^\s*(residuo|cantidad|tipo\s*tratamiento|tratamiento|destino|transportista|patente)\b", flags=re.IGNORECASE)
    for ln in lines:
        line = _clean_cell(ln)
        if not line:
            continue
        if header_noise.search(line) and "|" not in line and not ler_start.match(line):
            continue
        if "cantidad residuo tipo tratamiento destino" in _norm(line):
            continue
        if ler_start.match(line):
            if current:
                blocks.append(_clean_cell(" ".join(current)))
            current = [line]
        else:
            if current:
                current.append(line)
    if current:
        blocks.append(_clean_cell(" ".join(current)))
    return blocks


def _parse_reconstructed_row_block(
    block: str,
    known_treatments: Optional[List[str]] = None,
) -> Optional[Dict[str, str]]:
    def _find_catalog_match_spans(text: str, catalog: List[str]) -> List[Tuple[int, int, str]]:
        spans: List[Tuple[int, int, str]] = []
        for term in sorted(set(catalog), key=lambda x: len(x), reverse=True):
            if not _clean_cell(term):
                continue
            m = re.search(re.escape(term), text, flags=re.IGNORECASE)
            if m:
                spans.append((m.start(), m.end(), term))
        return spans

    def _is_destination_clean(dst_text: str, desc_text: str) -> bool:
        d = _norm(dst_text)
        if not d:
            return False
        if any(_norm(x) in d for x in DESTINATION_NOISE_FRAGMENTS):
            return False
        dn = _norm(desc_text)
        if dn:
            generic_tokens = {
                "residuos", "residuo", "plastico", "plasticos", "plástico", "plásticos", "envases", "organicos",
                "orgánicos", "tratamiento", "lodos", "subproductos", "fracciones", "especificadas",
            }
            for token in [t for t in dn.split() if len(t) >= 6]:
                if token in generic_tokens:
                    continue
                if token in d and token not in {"puerto", "tratamiento"}:
                    return False
        return True

    def _is_treatment_clean(treatment_text: str, desc_text: str) -> bool:
        t = _norm(treatment_text)
        if not t:
            return False
        if any(_norm(x) in t for x in TREATMENT_NOISE_FRAGMENTS):
            return False
        dn = _norm(desc_text)
        if dn:
            long_tokens = [tok for tok in dn.split() if len(tok) >= 8]
            if long_tokens and sum(1 for tok in long_tokens if tok in t) >= 2:
                return False
        return True

    def _parse_tail_right_to_left(tail: str, desc_text: str) -> Tuple[str, str, str, str, bool, bool]:
        text = _clean_cell(tail)
        if not text:
            return "", "", "", "", False, False
        pat = ""
        trp = ""
        m_marker = re.search(r"\b\d+\|\s*$", text)
        if m_marker:
            text = _clean_cell(text[:m_marker.start()])
        m_plate = re.search(r"\b((?=[A-Z0-9-]*\d)(?:[A-Z]{2,4}-[A-Z0-9]{2,4}|[A-Z]{2,4}[0-9]{2,4}))\b\s*$", text)
        if m_plate:
            candidate_plate = _clean_cell(m_plate.group(1)).replace(" ", "-")
            if not re.fullmatch(r"\d+\|?", candidate_plate):
                pat = candidate_plate
                text = _clean_cell(text[:m_plate.start()])

        treatment_catalog = list(dict.fromkeys(STRONG_TREATMENT_CATALOG + (known_treatments or [])))
        chosen_treatment = ""
        chosen_destination = ""
        destination_spans = _find_catalog_match_spans(text, KNOWN_DESTINATIONS)
        treatment_spans = _find_catalog_match_spans(text, treatment_catalog) if treatment_catalog else []
        best_pair: Optional[Tuple[Tuple[int, int, str], Tuple[int, int, str]]] = None
        best_pair_score = -1
        for t_span in treatment_spans:
            for d_span in destination_spans:
                overlap = not (t_span[1] <= d_span[0] or d_span[1] <= t_span[0])
                if overlap:
                    continue
                score = (t_span[1] - t_span[0]) + (d_span[1] - d_span[0])
                if t_span[0] <= d_span[0]:
                    score += 5
                if score > best_pair_score:
                    best_pair_score = score
                    best_pair = (t_span, d_span)

        if best_pair:
            t_span, d_span = best_pair
            chosen_treatment = t_span[2]
            chosen_destination = d_span[2]
            for span in sorted([t_span, d_span], key=lambda x: x[0], reverse=True):
                text = _clean_cell(text[:span[0]] + " " + text[span[1]:])
        else:
            if destination_spans:
                d_span = destination_spans[0]
                chosen_destination = d_span[2]
                text = _clean_cell(text[:d_span[0]] + " " + text[d_span[1]:])
            if treatment_spans:
                t_span = treatment_spans[0]
                chosen_treatment = t_span[2]
                text = _clean_cell(text[:t_span[0]] + " " + text[t_span[1]:])

        text = re.sub(r"\b(destino|transportista|patente|cantidad|residuo|tipo tratamiento)\b", " ", text, flags=re.IGNORECASE)
        text = _clean_cell(re.sub(r"\s+", " ", text))
        dst = chosen_destination or text
        trt_ok = bool(chosen_treatment) and _norm(chosen_treatment) in {_norm(x) for x in treatment_catalog} and _is_treatment_clean(chosen_treatment, desc_text)
        dst_ok = bool(chosen_destination) and _is_destination_clean(dst, desc_text)
        return chosen_treatment, dst, trp, pat, trt_ok, dst_ok

    block = _clean_cell(block)
    if not block:
        return None
    m_code = re.match(r"^\s*(\d{2}\s+\d{2}\s+\d{2})\s*\|\s*(.*)$", block)
    if not m_code:
        return None
    code = _clean_cell(m_code.group(1))
    rest = _clean_cell(m_code.group(2))
    m_qty = re.search(r"(?P<qty>\d[\d\.,]*)\s*kg\b", rest, flags=re.IGNORECASE)
    if not m_qty:
        return {
            "Código principal": code,
            "Descripción Residuo": rest,
            "Cantidad (Kg)": "",
            "Tratamiento": "",
            "Destino": "",
            "Transportista": "",
            "Patente": "",
            "Peligrosidad": "",
            "Estado contenedor": "",
            "Contenedor": "",
            "Texto fila original": block,
            "Parsing_OK": "NO",
            "Tratamiento_confiable": "NO",
            "Destino_confiable": "NO",
        }
    desc = _clean_cell(rest[:m_qty.start()])
    qty = _clean_cell(m_qty.group("qty"))
    tail = _clean_cell(rest[m_qty.end():])
    trt_raw, dst_raw, trp_raw, pat_raw, trt_ok, dst_ok = _parse_tail_right_to_left(tail, desc)
    trt, dst, trp, pat = _sanitize_treatment_and_logistics(
        trt_raw or tail,
        dst_raw,
        trp_raw,
        pat_raw,
        qty,
        known_treatments,
        desc,
    )
    qty_ok = _to_float_kg(qty) is not None
    code_ok = bool(code and re.match(r"^\d{2}\s+\d{2}\s+\d{2}$", code))
    semantic_ok = (trt_ok or dst_ok) and _is_destination_clean(dst, desc) and (not trt or _is_treatment_clean(trt, desc))
    parsing_ok = bool(code_ok and desc and qty_ok and semantic_ok)
    return {
        "Código principal": code,
        "Descripción Residuo": desc,
        "Cantidad (Kg)": qty,
        "Tratamiento": trt,
        "Destino": dst,
        "Transportista": trp,
        "Patente": pat,
        "Peligrosidad": "",
        "Estado contenedor": "",
        "Contenedor": "",
        "Texto fila original": block,
        "Parsing_OK": "SI" if parsing_ok else "NO",
        "Tratamiento_confiable": "SI" if trt_ok and bool(trt) else "NO",
        "Destino_confiable": "SI" if dst_ok and bool(dst) else "NO",
    }


def parse_sinader_rows_from_tables(pdf_path: str) -> List[Dict[str, str]]:
    rows_out: List[Dict[str, str]] = []
    known_treatments = load_treatment_level3_terms()
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            try:
                tables = page.extract_tables(table_settings=STREAM_SETTINGS) or []
            except Exception:
                tables = page.extract_tables() or []
            for tb in tables:
                if not tb or not tb[0]:
                    continue
                if not _looks_like_sinader_table(tb):
                    continue
                raw_lines: List[str] = []
                for r in tb[1:]:
                    cells = [_clean_cell(c) for c in (r or []) if _clean_cell(c)]
                    if not cells:
                        continue
                    raw_lines.append(" | ".join(cells))
                blocks = _reconstruct_row_blocks_from_lines(raw_lines)
                for block in blocks:
                    parsed = _parse_reconstructed_row_block(block, known_treatments)
                    if not parsed:
                        continue
                    rows_out.append(parsed)
    uniq = {}
    for r in rows_out:
        key = (r.get("Código principal", ""), _norm(r.get("Descripción Residuo", "")), _clean_cell(r.get("Cantidad (Kg)", "")))
        uniq.setdefault(key, r)
    return list(uniq.values())


def parse_sinader_rows_from_text(full_text: str) -> List[Dict[str, str]]:
    rows_out: List[Dict[str, str]] = []
    if not full_text:
        return rows_out
    known_treatments = load_treatment_level3_terms()
    block_text = _extract_table_text_block(full_text)
    lines = [_clean_cell(x) for x in block_text.splitlines() if _clean_cell(x)]
    blocks = _reconstruct_row_blocks_from_lines(lines)
    for block in blocks:
        parsed = _parse_reconstructed_row_block(block, known_treatments)
        if parsed:
            rows_out.append(parsed)
    uniq = {}
    for r in rows_out:
        key = (r.get("Código principal", ""), _norm(r.get("Descripción Residuo", "")), _clean_cell(r.get("Cantidad (Kg)", "")))
        uniq.setdefault(key, r)
    return list(uniq.values())


def extract_global_treatment_from_text(full_text: str, known_treatments: Optional[List[str]] = None) -> str:
    text = _cell_join_multiline(full_text or "")
    if not text:
        return ""
    text_norm = _norm(text)
    if known_treatments:
        for term in sorted(known_treatments, key=lambda x: len(x), reverse=True):
            term_norm = _norm(term)
            if term_norm and term_norm in text_norm:
                return term
    patterns = [
        r"(?:tipo\s*tratamiento|tratamiento)\s*[:\-]?\s*(reutilizaci[oó]n|reciclaje|combusti[oó]n|vertedero|anaerobic digestion)",
        r"(?:tipo\s*tratamiento|tratamiento)\s*[:\-]?\s*([A-Za-zÁÉÍÓÚÜÑáéíóúüñ\s]{4,60})",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            return _clean_cell(m.group(1))
    return ""


def _sanitize_treatment_and_logistics(
    tratamiento: str,
    destino: str,
    transportista: str,
    patente: str,
    cantidad: str = "",
    known_treatments: Optional[List[str]] = None,
    descripcion: str = "",
) -> Tuple[str, str, str, str]:
    def _extract_treatment_phrase(text: str) -> str:
        if not text:
            return ""
        text_norm = _norm(text)
        if known_treatments:
            for term in sorted(known_treatments, key=lambda x: len(x), reverse=True):
                term_norm = _norm(term)
                if not term_norm:
                    continue
                if term_norm in text_norm:
                    return term
                term_tokens = [t for t in term_norm.split() if len(t) > 3]
                if term_tokens and all(t in text_norm for t in term_tokens):
                    return term
        if "degradacion" in text_norm and "anaerobica" in text_norm:
            return "Degradación Anaeróbica"
        candidates = [
            r"relleno\s+sanitario",
            r"sitio\s+de\s+escombros\s+de\s+la\s+construcci[oó]n",
            r"reciclaje\s+de\s+pl[aá]sticos",
            r"reciclaje\s+de\s+metales",
            r"reciclaje\s+de\s+papel(?:,\s*cart[oó]n\s*y\s*productos\s*de\s*papel)?",
            r"monorelleno",
            r"compostaje",
            r"reutilizaci[oó]n",
            r"combusti[oó]n",
            r"anaerobic\s+digestion",
            r"reciclaje",
        ]
        for pat in candidates:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if m:
                return _clean_cell(m.group(0))
        return ""

    trt = _clean_cell(tratamiento)
    dst = _clean_cell(destino)
    trp = _clean_cell(transportista)
    pat = _clean_cell(patente)
    raw_combined = f"{trt} {dst}".strip()

    if trt or dst:
        def _is_placeholder_destination(value: str) -> bool:
            v = _norm(value)
            return (not v) or v.startswith("in situ") or v.startswith("situ de efluentes")

        def _clean_destination_noise(value: str) -> str:
            cleaned = _clean_cell(value)
            original_norm = _norm(cleaned)
            for known_dst in sorted(KNOWN_DESTINATIONS, key=lambda x: len(x), reverse=True):
                kd_norm = _norm(known_dst)
                if kd_norm and kd_norm in original_norm:
                    return known_dst
            for frag in DESTINATION_NOISE_FRAGMENTS:
                cleaned = re.sub(re.escape(frag), " ", cleaned, flags=re.IGNORECASE)
            if descripcion:
                desc_norm = _norm(descripcion)
                for token in [t for t in desc_norm.split() if len(t) >= 7]:
                    if token in {"tratamiento", "residuos", "categoria", "especificadas"}:
                        continue
                    cleaned = re.sub(rf"\b{re.escape(token)}\b", " ", cleaned, flags=re.IGNORECASE)
            cleaned = _clean_cell(re.sub(r"\s+", " ", cleaned))
            cleaned_norm = _norm(cleaned)
            for known_dst in sorted(KNOWN_DESTINATIONS, key=lambda x: len(x), reverse=True):
                kd_norm = _norm(known_dst)
                if kd_norm and kd_norm in cleaned_norm:
                    return known_dst
            return cleaned

        def _tail_after_qty_kg(text: str, qty_value: str) -> str:
            if not text:
                return ""
            qty_digits = re.sub(r"\D", "", _clean_cell(qty_value))
            matches = list(re.finditer(r"(\d[\d\.,]*)\s*kg\b", text, flags=re.IGNORECASE))
            if not matches:
                return ""
            if not qty_digits:
                return _clean_cell(text[matches[0].end():])
            for m in matches:
                m_digits = re.sub(r"\D", "", m.group(1))
                if m_digits == qty_digits:
                    return _clean_cell(text[m.end():])
            return ""

        tail_by_qty = _tail_after_qty_kg(raw_combined, cantidad)
        if tail_by_qty:
            phrase_from_tail = _extract_treatment_phrase(tail_by_qty)
            if phrase_from_tail:
                trt = phrase_from_tail
                remainder_tail = _clean_cell(re.sub(re.escape(phrase_from_tail), "", tail_by_qty, count=1, flags=re.IGNORECASE))
                if remainder_tail and _is_placeholder_destination(dst):
                    dst = remainder_tail

        kg_split = re.search(r"^(?P<prefix>.*?)(?P<qty>\d[\d\.,]*)\s*kg\s*(?P<after>.*)$", trt, flags=re.IGNORECASE)
        if kg_split:
            trt = _clean_cell(kg_split.group("prefix"))
            trailing = _clean_cell(kg_split.group("after"))
            if trailing and _is_placeholder_destination(dst):
                dst = trailing
        trt = re.sub(r"^\d[\d\.,]*\s*(kg|kgs?)\s*", "", trt, flags=re.IGNORECASE).strip()
        if _norm(trt) in {"destino transportista patente", "destino transportista", "transportista patente"}:
            trt = ""
        if "|" in trt and not dst:
            left, right = [x.strip() for x in trt.split("|", 1)]
            if right:
                trt = left
                dst = right

        labeled = re.search(
            r"(?:^|\s)destino\s*[:\-]?\s*(?P<dst>.*?)(?:\s+transportista\s*[:\-]?\s*(?P<trp>.*?))?(?:\s+patente\s*[:\-]?\s*(?P<pat>.*))?$",
            trt,
            flags=re.IGNORECASE,
        )
        if labeled:
            if not dst:
                dst = _clean_cell(labeled.group("dst") or "")
            if not trp:
                trp = _clean_cell(labeled.group("trp") or "")
            if not pat:
                pat = _clean_cell(labeled.group("pat") or "")
            trt = ""

        phrase = _extract_treatment_phrase(trt)
        if not phrase and raw_combined:
            phrase = _extract_treatment_phrase(raw_combined)
        if phrase:
            remainder = _clean_cell(re.sub(re.escape(phrase), "", trt, count=1, flags=re.IGNORECASE))
            if remainder and _is_placeholder_destination(dst):
                dst = remainder
            if dst:
                dst = _clean_cell(re.sub(re.escape(phrase), "", dst, count=1, flags=re.IGNORECASE))
            trt = phrase

        combined_all = _clean_cell(" ".join(x for x in [trt, dst, trp, pat] if _clean_cell(x)))
        phrase_all = _extract_treatment_phrase(combined_all) if combined_all else ""
        if phrase_all:
            trt = phrase_all
            cleaned = combined_all
            cleaned = re.sub(re.escape(phrase_all), " ", cleaned, count=1, flags=re.IGNORECASE)
            cleaned = re.sub(r"\d[\d\.,]*\s*kg\b", " ", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\b(destino|transportista|patente)\b", " ", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\bin\s*situ\s*de\s*efluentes\b", " ", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\d+\|", " ", cleaned)
            cleaned = re.sub(r"\b\d+\b", " ", cleaned)
            if not pat:
                m_pat = re.search(r"\b((?=[A-Z0-9-]*\d)(?:[A-Z]{2,4}-[A-Z0-9]{2,4}|[A-Z]{2,4}[0-9]{2,4}))\b", cleaned)
                if m_pat:
                    pat = m_pat.group(1)
                    cleaned = cleaned.replace(m_pat.group(1), " ")
            candidate_dst = _clean_cell(re.sub(r"\s+", " ", cleaned).strip(" |-_/"))
            if candidate_dst and _is_placeholder_destination(dst):
                dst = candidate_dst

        if dst:
            dst = re.sub(r"\d+\|", " ", dst)
            dst = _clean_destination_noise(_clean_cell(re.sub(r"\s+", " ", dst)))

    return trt, dst, trp, pat


def extract_sinader_from_pdf(pdf_path: str) -> Tuple[List[Dict[str, str]], Dict[str, str]]:
    with pdfplumber.open(pdf_path) as pdf:
        full_text = "\n".join([(p.extract_text() or "") for p in pdf.pages])
    meta = extract_sinader_metadata(full_text, pdf_path)
    if sinader_has_no_movements(full_text):
        return [{
            "N.": "0",
            "Descripción Residuo": "PERÍODO SIN MOVIMIENTOS",
            "Código principal": "",
            "Peligrosidad": "",
            "Cantidad (Kg)": "0",
            "Estado contenedor": "",
            "Contenedor": "",
            "Tratamiento": "",
            "Destino": "",
            "Transportista": "",
            "Patente": "",
            "Sin movimientos": "SI",
            "Texto fila original": "",
            "Parsing_OK": "SI",
            "Tratamiento_confiable": "NO",
            "Destino_confiable": "NO",
        }], meta
    rows_from_tables = parse_sinader_rows_from_tables(pdf_path)
    rows_from_text = parse_sinader_rows_from_text(full_text)

    def _score_rows(rows: List[Dict[str, str]]) -> tuple[int, int, int]:
        valid_core = sum(1 for r in rows if _clean_cell(r.get("Código principal", "")) and _clean_cell(r.get("Cantidad (Kg)", "")))
        with_treatment = sum(1 for r in rows if _clean_cell(r.get("Tratamiento", "")))
        return (valid_core, with_treatment, len(rows))

    detail_rows = rows_from_tables
    if not rows_from_tables and rows_from_text:
        detail_rows = rows_from_text
    elif rows_from_tables and rows_from_text:
        detail_rows = rows_from_text if _score_rows(rows_from_text) > _score_rows(rows_from_tables) else rows_from_tables

    known_treatments = load_treatment_level3_terms()
    global_treatment = extract_global_treatment_from_text(full_text, known_treatments)
    out_rows = []
    for i, r in enumerate(detail_rows, start=1):
        row_treatment = _clean_cell(r.get("Tratamiento", ""))
        if not row_treatment and global_treatment:
            row_treatment = global_treatment
        row_treatment, row_destino, row_transportista, row_patente = _sanitize_treatment_and_logistics(
            row_treatment,
            r.get("Destino", ""),
            r.get("Transportista", ""),
            r.get("Patente", ""),
            r.get("Cantidad (Kg)", ""),
            known_treatments,
            r.get("Descripción Residuo", ""),
        )
        out_rows.append({
            "N.": str(i),
            "Descripción Residuo": r.get("Descripción Residuo", ""),
            "Código principal": r.get("Código principal", ""),
            "Peligrosidad": r.get("Peligrosidad", ""),
            "Cantidad (Kg)": r.get("Cantidad (Kg)", ""),
            "Estado contenedor": r.get("Estado contenedor", ""),
            "Contenedor": r.get("Contenedor", ""),
            "Tratamiento": row_treatment,
            "Destino": row_destino,
            "Transportista": row_transportista,
            "Patente": row_patente,
            "Sin movimientos": "NO",
            "Texto fila original": r.get("Texto fila original", ""),
            "Parsing_OK": r.get("Parsing_OK", "NO"),
            "Tratamiento_confiable": r.get("Tratamiento_confiable", "NO"),
            "Destino_confiable": r.get("Destino_confiable", "NO"),
        })
    return out_rows, meta


def _normalize_for_match(s: str) -> str:
    s = _strip_accents(s or "").lower().replace("|", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _prefix_similarity(extracted_text: str, canonical_text: str) -> float:
    a = _normalize_for_match(extracted_text)
    b = _normalize_for_match(canonical_text)
    if not a or not b:
        return 0.0
    if b.startswith(a):
        return 1.0
    best = SequenceMatcher(None, a, b).ratio()
    words = b.split()
    for i in range(1, len(words) + 1):
        best = max(best, SequenceMatcher(None, a, " ".join(words[:i])).ratio())
    return best


def choose_canonical_description(extracted_desc: str, codigo: str, catalog: Dict[str, List[str]], threshold: float = 0.72) -> str:
    extracted_desc = _clean_cell(extracted_desc)
    codigo = _clean_cell(codigo)
    if not extracted_desc:
        return extracted_desc
    same_code_candidates = catalog.get(codigo, [])
    if len(same_code_candidates) == 1:
        return same_code_candidates[0]
    if same_code_candidates:
        scored = sorted([(cand, _prefix_similarity(extracted_desc, cand)) for cand in same_code_candidates], key=lambda x: x[1], reverse=True)
        if scored[0][1] >= threshold:
            return scored[0][0]
    all_candidates = [d for descs in catalog.values() for d in descs]
    if not all_candidates:
        return extracted_desc
    scored = sorted([(cand, _prefix_similarity(extracted_desc, cand)) for cand in all_candidates], key=lambda x: x[1], reverse=True)
    return scored[0][0] if scored[0][1] >= threshold else extracted_desc


def _build_catalog_from_dataframe(df: pd.DataFrame) -> Dict[str, List[str]]:
    if df.empty:
        return {}
    normalized_cols = {_norm(c): c for c in df.columns}
    code_col = None
    desc_col = None
    code_candidates = [
        "codigo principal",
        "código principal",
        "codigo",
        "código",
        "codigo residuo",
        "código residuo",
        "codigo sinader",
        "código sinader",
        "codigo ler",
        "código ler",
    ]
    desc_candidates = [
        "descripcion residuo",
        "descripción residuo",
        "descripcion",
        "descripción",
        "residuo",
        "entry official name (en)",
        "entry official name",
        "capitulo oficial sinader (es)",
        "subchapter official name (en)",
    ]
    declarable_col = None
    declarable_candidates = [
        "declarable en sinader?",
        "¿declarable en sinader?",
        "declarable en sinader",
    ]
    for candidate in code_candidates:
        if candidate in normalized_cols:
            code_col = normalized_cols[candidate]
            break
    for candidate in desc_candidates:
        if candidate in normalized_cols:
            desc_col = normalized_cols[candidate]
            break
    for candidate in declarable_candidates:
        if candidate in normalized_cols:
            declarable_col = normalized_cols[candidate]
            break
    if not code_col or not desc_col:
        return {}
    catalog: Dict[str, List[str]] = {}
    required_cols = [code_col, desc_col] + ([declarable_col] if declarable_col else [])
    for _, row in df[required_cols].dropna(subset=[code_col, desc_col]).iterrows():
        if declarable_col:
            declarable_value = _norm(_clean_cell(row.get(declarable_col, "")))
            if declarable_value and declarable_value not in {"si", "sí", "s", "yes", "true"}:
                continue
        code = _normalize_code(row[code_col])
        desc = _clean_cell(row[desc_col])
        if not code or not desc:
            continue
        catalog.setdefault(code, [])
        if desc not in catalog[code]:
            catalog[code].append(desc)
    return catalog


def load_residuo_catalog(catalog_path: Optional[str] = None) -> Dict[str, List[str]]:
    configured_path = (catalog_path or os.getenv("SINADER_CATALOG_PATH", "")).strip()
    candidate_paths = [Path(configured_path)] if configured_path else []
    candidate_paths.append(DEFAULT_CATALOG_PATH)
    for path in candidate_paths:
        if not path.exists() or not path.is_file():
            continue
        try:
            excel_file = pd.ExcelFile(path)
            sheet_candidates = [s for s in PREFERRED_CATALOG_SHEETS if s in excel_file.sheet_names]
            sheet_candidates.extend([s for s in excel_file.sheet_names if s not in sheet_candidates])
            for sheet_name in sheet_candidates:
                df = pd.read_excel(path, sheet_name=sheet_name)
                catalog = _build_catalog_from_dataframe(df)
                if catalog:
                    logger.info("Catálogo SINADER cargado desde %s (hoja=%s, códigos=%s)", path, sheet_name, len(catalog))
                    return catalog
            logger.warning("Catálogo SINADER en %s no tiene columnas válidas de código/descripcion", path)
        except Exception as exc:
            logger.warning("No se pudo cargar catálogo SINADER en %s: %s", path, exc)
    return MASTER_RESIDUOS


def _build_treatment_defra_map_from_dataframe(df: pd.DataFrame) -> Dict[str, str]:
    if df.empty:
        return {}
    normalized_cols = {_norm(c): c for c in df.columns}
    defra_col = None
    treatment_col = None
    defra_candidates = [
        "defra",
        "nombre defra",
        "nombre defra (lista 1 exacta)",
        "defra name",
    ]
    treatment_candidates = [
        "tratamiento sinader",
        "tratamiento",
        "tratamiento sinader (es)",
        "tipo tratamiento",
        "sinader",
    ]
    for candidate in defra_candidates:
        if candidate in normalized_cols:
            defra_col = normalized_cols[candidate]
            break
    for candidate in treatment_candidates:
        if candidate in normalized_cols:
            treatment_col = normalized_cols[candidate]
            break
    if not defra_col or not treatment_col:
        cols = list(df.columns[:2])
        if len(cols) >= 2:
            defra_col, treatment_col = cols[0], cols[1]
        else:
            return {}
    mapping: Dict[str, str] = {}
    for _, row in df[[defra_col, treatment_col]].dropna(how="any").iterrows():
        defra_name = _clean_cell(row[defra_col])
        treatment_name = _norm(_clean_cell(row[treatment_col]))
        if not defra_name or not treatment_name:
            continue
        mapping[treatment_name] = defra_name
    return mapping


def load_treatment_defra_map(catalog_path: Optional[str] = None) -> Dict[str, str]:
    configured_path = (catalog_path or os.getenv("SINADER_CATALOG_PATH", "")).strip()
    candidate_paths = [Path(configured_path)] if configured_path else []
    candidate_paths.append(DEFAULT_CATALOG_PATH)
    for path in candidate_paths:
        if not path.exists() or not path.is_file():
            continue
        try:
            excel_file = pd.ExcelFile(path)
            if TREATMENT_CATALOG_SHEET not in excel_file.sheet_names:
                continue
            df = pd.read_excel(path, sheet_name=TREATMENT_CATALOG_SHEET)
            mapping = _build_treatment_defra_map_from_dataframe(df)
            if mapping:
                logger.info("Mapa Tratamiento->DEFRA cargado desde %s (hoja=%s, filas=%s)", path, TREATMENT_CATALOG_SHEET, len(mapping))
                return mapping
        except Exception as exc:
            logger.warning("No se pudo cargar mapa de tratamientos SINADER en %s: %s", path, exc)
    return dict(DEFAULT_TREATMENT_DEFRA_MAP)


def load_treatment_level3_terms(catalog_path: Optional[str] = None) -> List[str]:
    configured_path = (catalog_path or os.getenv("SINADER_CATALOG_PATH", "")).strip()
    candidate_paths = [Path(configured_path)] if configured_path else []
    candidate_paths.append(DEFAULT_CATALOG_PATH)
    for path in candidate_paths:
        if not path.exists() or not path.is_file():
            continue
        try:
            excel_file = pd.ExcelFile(path)
            if TREATMENT_CATALOG_SHEET not in excel_file.sheet_names:
                continue
            df = pd.read_excel(path, sheet_name=TREATMENT_CATALOG_SHEET)
            normalized_cols = {_norm(c): c for c in df.columns}
            level3_col = None
            for candidate in ["nivel 3", "nivel3", "level 3", "tratamiento", "treatment"]:
                if candidate in normalized_cols:
                    level3_col = normalized_cols[candidate]
                    break
            if not level3_col:
                continue
            values = []
            for value in df[level3_col].dropna().tolist():
                text = _clean_cell(value)
                if text:
                    values.append(text)
            unique_values = sorted(set(values), key=lambda x: len(x), reverse=True)
            if unique_values:
                logger.info("Tratamientos Nivel 3 cargados desde %s (hoja=%s, filas=%s)", path, TREATMENT_CATALOG_SHEET, len(unique_values))
                return unique_values
        except Exception as exc:
            logger.warning("No se pudo cargar tratamientos Nivel 3 desde %s: %s", path, exc)
    return []


def map_treatment_to_defra(tratamiento: str, treatment_map: Dict[str, str]) -> str:
    normalized_treatment = _norm(tratamiento)
    if not normalized_treatment:
        return ""
    if normalized_treatment in treatment_map:
        return treatment_map[normalized_treatment]
    for key, defra_value in treatment_map.items():
        if key in normalized_treatment:
            return defra_value
    return ""


def choose_canonical_treatment(extracted_treatment: str, known_treatments: List[str], threshold: float = 0.58) -> str:
    raw = _clean_cell(extracted_treatment)
    if not raw or not known_treatments:
        return raw
    a = _normalize_for_match(raw)
    if not a:
        return raw
    best_term = raw
    best_score = 0.0
    for term in known_treatments:
        b = _normalize_for_match(term)
        if not b:
            continue
        score = 1.0 if (a in b or b in a) else SequenceMatcher(None, a, b).ratio()
        if score > best_score:
            best_score = score
            best_term = term
    return best_term if best_score >= threshold else raw


def load_treatment_alias_map(training_files: Optional[List[str]] = None) -> Dict[str, str]:
    files = training_files or sorted(glob.glob(DEFAULT_TREATMENT_TRAINING_GLOB))
    alias_map: Dict[str, str] = {}
    for file_path in files:
        try:
            df = pd.read_excel(file_path)
        except Exception:
            continue
        if df.empty:
            continue
        normalized_cols = {_norm(c): c for c in df.columns}
        extracted_col = None
        expected_col = None
        extracted_candidates = ["tratamiento", "tratamiento actual", "tratamiento extraido", "tratamiento extraído"]
        expected_candidates = ["tratamiento esperado", "esperado tratamiento", "tratamiento objetivo", "tratamiento correcto"]
        for c in extracted_candidates:
            if c in normalized_cols:
                extracted_col = normalized_cols[c]
                break
        for c in expected_candidates:
            if c in normalized_cols:
                expected_col = normalized_cols[c]
                break
        if not extracted_col or not expected_col:
            continue
        for _, row in df[[extracted_col, expected_col]].dropna(how="any").iterrows():
            src = _norm(_clean_cell(row[extracted_col]))
            dst = _clean_cell(row[expected_col])
            if src and dst:
                alias_map[src] = dst
    if alias_map:
        logger.info("Mapa de alias de tratamiento cargado desde salidas históricas (%s reglas)", len(alias_map))
    return alias_map


def apply_residuo_dictionary_correction(df: pd.DataFrame, catalog: Dict[str, List[str]]) -> pd.DataFrame:
    if "Descripción Residuo" not in df.columns or "Código principal" not in df.columns:
        return df
    df = df.copy()
    if "Descripción Residuo Original" not in df.columns:
        df["Descripción Residuo Original"] = df["Descripción Residuo"]
    df["Código principal"] = df["Código principal"].apply(_normalize_code)
    df["Descripción Residuo"] = df.apply(
        lambda r: choose_canonical_description(
            r.get("Descripción Residuo", ""),
            r.get("Código principal", ""),
            catalog,
        ),
        axis=1,
    )
    return df


def defra_classification(desc_residuo: str, sin_movimientos: str = "", codigo_principal: str = "", tratamiento: str = "", destino: str = "") -> str:
    d = _norm(desc_residuo)
    cod = _clean_cell(codigo_principal)
    t = _norm(tratamiento)
    dst = _norm(destino)
    ctx = " ".join(x for x in [d, t, dst] if x).strip()
    if _norm(sin_movimientos) in ("si", "sí") or "periodo sin movimientos" in d or "período sin movimientos" in d:
        return "NA"
    def has_any(*terms: str) -> bool:
        return any(_norm(term) in ctx for term in terms)
    if cod == "15 01 01":
        return "Paper and board: mixed"
    if cod == "15 01 04":
        return "Metals"
    if cod == "21 04 04":
        if has_any("hdpe"):
            return "Plastics: HDPE (incl. forming)"
        if has_any("ldpe", "lldpe", "pee"):
            return "Plastics: LDPE and LLDPE (incl. forming)"
        if has_any("pet", "pete"):
            return "Plastics: PET (incl. forming)"
        if has_any("pp"):
            return "Plastics: PP (incl. forming)"
        if has_any("ps"):
            return "Plastics: PS (incl. forming)"
        if has_any("pvc"):
            return "Plastics: PVC (incl. forming)"
        return "Plastics: average plastics"
    if cod == "15 01 02":
        if has_any("film", "lamina", "lámina", "saco", "bolsa", "stretch"):
            return "Plastics: average plastic film"
        if has_any("hdpe"):
            return "Plastics: HDPE (incl. forming)"
        if has_any("ldpe", "lldpe", "pee"):
            return "Plastics: LDPE and LLDPE (incl. forming)"
        if has_any("pet", "pete"):
            return "Plastics: PET (incl. forming)"
        if has_any("pp"):
            return "Plastics: PP (incl. forming)"
        if has_any("ps"):
            return "Plastics: PS (incl. forming)"
        if has_any("pvc"):
            return "Plastics: PVC (incl. forming)"
        return "Plastics: average plastic rigid"
    if cod == "02 01 99":
        return "Organic: mixed food and garden waste"
    if cod == "02 02 04":
        return "Organic: food and drink waste"
    if cod == "19 08 05":
        return "Commercial and industrial waste"
    if cod == "20 01 99":
        return "Household residual waste" if has_any("relleno sanitario", "residuo domiciliario", "residual") else "Commercial and industrial waste"
    if cod == "10 01 01":
        return "Commercial and industrial waste"
    if has_any("envases de papel y carton", "envases de papel y cartón"):
        return "Paper and board: mixed"
    if has_any("papel") and not has_any("carton", "cartón", "board"):
        return "Paper and board: paper"
    if has_any("carton", "cartón", "board") and not has_any("papel"):
        return "Paper and board: board"
    if has_any("envases metalicos", "envases metálicos", "metal", "acero", "chatarra"):
        return "Metals"
    if has_any("hdpe"):
        return "Plastics: HDPE (incl. forming)"
    if has_any("ldpe", "lldpe", "pee"):
        return "Plastics: LDPE and LLDPE (incl. forming)"
    if has_any("pet", "pete"):
        return "Plastics: PET (incl. forming)"
    if has_any("pp"):
        return "Plastics: PP (incl. forming)"
    if has_any("ps"):
        return "Plastics: PS (incl. forming)"
    if has_any("pvc"):
        return "Plastics: PVC (incl. forming)"
    if has_any("plastico", "plástico"):
        if has_any("film", "lamina", "lámina", "saco", "bolsa", "stretch"):
            return "Plastics: average plastic film"
        if has_any("envase", "bidon", "bidón", "tambor", "contenedor"):
            return "Plastics: average plastic rigid"
        return "Plastics: average plastics"
    if has_any("compost", "organico", "orgánico", "resto de alimento", "restos de alimento"):
        return "Organic: mixed food and garden waste"
    if has_any("lodos del tratamiento in situ de efluentes", "degradacion anaerobica", "degradación anaeróbica"):
        return "Organic: food and drink waste"
    if has_any("aguas residuales urbanas", "ptas"):
        return "Commercial and industrial waste"
    if has_any("ceniza", "escoria", "caldera"):
        return "Commercial and industrial waste"
    if has_any("relleno sanitario", "fracciones no especificadas", "residuos no especificados"):
        return "Household residual waste"
    return "Commercial and industrial waste"


def extract_any_pdf(pdf_path: str) -> Tuple[List[Dict[str, str]], Dict[str, str], str]:
    with pdfplumber.open(pdf_path) as pdf:
        full_text = "\n".join([(p.extract_text() or "") for p in pdf.pages])
    if is_sinader_pdf(full_text):
        rows, meta = extract_sinader_from_pdf(pdf_path)
        return rows, meta, "SINADER"
    return [], {"FuentePDF": Path(pdf_path).name}, "UNKNOWN"


def process_folder(input_folder: str, output_excel: str) -> pd.DataFrame:
    pdf_paths = sorted([str(p) for p in Path(input_folder).rglob("*.pdf")])
    if not pdf_paths:
        raise FileNotFoundError(f"No se encontraron PDFs en: {input_folder}")
    all_rows: List[Dict[str, str]] = []
    for p in pdf_paths:
        try:
            rows, meta, kind = extract_any_pdf(p)
            logger.info("Procesado: %s | tipo=%s | filas_detalle=%s", Path(p).name, kind, len(rows))
            for r in rows:
                merged = dict(meta)
                merged.update(r)
                merged["TipoPDF"] = kind
                all_rows.append(merged)
        except Exception as e:
            logger.exception("Error procesando %s: %s", p, e)
    df = pd.DataFrame(all_rows)
    preferred_cols = [
        "FuentePDF", "TipoPDF", "Periodo declarado", "Folio", "Establecimiento", "Razón social",
        "RUT Titular", "Realizado por", "Tipo", "Estado", "Código identificador", "Región", "Comuna",
        "Sin movimientos", "N.", "Descripción Residuo", "Descripción Residuo Original", "Código principal",
        "Peligrosidad", "Cantidad (Kg)", "Tratamiento", "Destino", "Transportista", "Patente",
        "Contenedor", "Estado contenedor", "Texto fila original", "Parsing_OK", "Tratamiento_confiable", "Destino_confiable", "DEFRA_base", "DEFRA",
    ]
    cols = [c for c in preferred_cols if c in df.columns] + [c for c in df.columns if c not in preferred_cols]
    df = df[cols] if not df.empty else pd.DataFrame(columns=preferred_cols)
    if "Cantidad (Kg)" in df.columns:
        df["Cantidad (Kg)"] = df["Cantidad (Kg)"].apply(_to_float_kg)
    catalog = load_residuo_catalog()
    df = apply_residuo_dictionary_correction(df, catalog)
    known_treatments = load_treatment_level3_terms()
    treatment_alias_map = load_treatment_alias_map()
    if "Tratamiento" in df.columns and known_treatments:
        if "Tratamiento Original" not in df.columns:
            df["Tratamiento Original"] = df["Tratamiento"]
        df["Tratamiento"] = df["Tratamiento"].apply(
            lambda x: treatment_alias_map.get(_norm(_clean_cell(x)), choose_canonical_treatment(x, known_treatments))
        )
    treatment_defra_map = load_treatment_defra_map()
    if "DEFRA" not in df.columns:
        df["DEFRA"] = ""
    df["DEFRA_base"] = df.apply(
        lambda r: defra_classification(
            desc_residuo=r.get("Descripción Residuo", ""),
            sin_movimientos=r.get("Sin movimientos", ""),
            codigo_principal=r.get("Código principal", ""),
            tratamiento="",
            destino="",
        ),
        axis=1,
    )
    df["DEFRA"] = df["DEFRA_base"]
    if "Tratamiento" in df.columns:
        def _treatment_is_reliable(row) -> bool:
            t = _norm(_clean_cell(row.get("Tratamiento", "")))
            txt = _norm(_clean_cell(row.get("Texto fila original", "")))
            flag = _norm(_clean_cell(row.get("Tratamiento_confiable", "no")))
            if flag not in {"si", "yes", "true"}:
                return False
            if not t:
                return False
            bad_tokens = ["cantidad residuo", "tipo tratamiento destino", "transportista patente", "destino transportista"]
            if any(b in t for b in bad_tokens):
                return False
            if any(b in txt for b in bad_tokens):
                return False
            return True

        df["DEFRA"] = df.apply(
            lambda r: (
                map_treatment_to_defra(r.get("Tratamiento", ""), treatment_defra_map)
                if _treatment_is_reliable(r)
                else ""
            ) or r.get("DEFRA_base", ""),
            axis=1,
        )
    Path(output_excel).parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_excel, index=False)
    logger.info("Excel generado: %s | filas=%s", output_excel, len(df))
    return df


def summarize_parsing_quality(df: pd.DataFrame, known_treatments: Optional[List[str]] = None) -> Dict[str, int]:
    if df is None or df.empty:
        return {
            "rows_total": 0,
            "rows_parsing_no": 0,
            "rows_destino_contaminado": 0,
            "rows_tratamiento_fuera_catalogo": 0,
        }
    known_treatments = known_treatments or load_treatment_level3_terms() or STRONG_TREATMENT_CATALOG
    known_treatment_norm = {_norm(t) for t in known_treatments if _clean_cell(t)}
    forbidden_dst = [_norm(x) for x in DESTINATION_NOISE_FRAGMENTS]
    forbidden_trt = [_norm(x) for x in TREATMENT_NOISE_FRAGMENTS]

    def _dst_contaminated(row: pd.Series) -> bool:
        dst = _norm(_clean_cell(row.get("Destino", "")))
        if not dst:
            return False
        if any(f and f in dst for f in forbidden_dst):
            return True
        desc = _norm(_clean_cell(row.get("Descripción Residuo", "")))
        desc_tokens = [tok for tok in desc.split() if len(tok) >= 8]
        return sum(1 for tok in desc_tokens if tok in dst) >= 2

    def _trt_outside_catalog(row: pd.Series) -> bool:
        trt = _norm(_clean_cell(row.get("Tratamiento", "")))
        if not trt:
            return False
        if any(f and f in trt for f in forbidden_trt):
            return True
        return trt not in known_treatment_norm

    return {
        "rows_total": int(len(df)),
        "rows_parsing_no": int((df.get("Parsing_OK", pd.Series(dtype=str)).fillna("NO").astype(str).str.upper() != "SI").sum()),
        "rows_destino_contaminado": int(df.apply(_dst_contaminated, axis=1).sum()),
        "rows_tratamiento_fuera_catalogo": int(df.apply(_trt_outside_catalog, axis=1).sum()),
    }


def _selfcheck_reconstruction_samples() -> Dict[str, bool]:
    sample_lines = [
        "Residuo Cantidad (kg) Tipo Tratamiento Destino Transportista Patente",
        "02 02 04 | Lodos del tratamiento in",
        "situ de efluentes 26450 kg Degradación",
        "Anaeróbica ECOPRIAL 1|",
        "19 08 05 | Lodos del tratamiento de aguas residuales urbanas 84180 kg Recepción de Lodos en PTAS PLANTA DE TRATAMIENTO DE AGUAS SERVIDAS DE CASTRO 1|",
        "15 01 01 | Envases de papel y cartón 165 kg Reciclaje de papel, cartón y productos de papel ECOFIBRAS SUCURSAL PUERTO MONTT 1|",
        "10 01 01 | Cenizas del hogar 4260 kg Sitio de Escombros de la Construcción ESCOMBRERA TRESOL 1|",
        "20 01 99 | Otras fracciones no especificadas en otra categoría 4210 kg Relleno sanitario CONSORCIO COLLIPULLI 1|",
        "21 04 04 | Residuos de plásticos (HDPE, PEE, PETE, PVC) excepto planzas, boyas, flotadores, redes y cabos 29756 kg Reciclaje de plásticos PLASTICOS DEL SUR SPA 1|",
        "02 01 99 | Residuos no especificados en otra categoría 8620 kg Compostaje Centro Crucero 1|",
        "02 02 03 | Subproductos hidrobiológicos 9000 kg Reciclaje de residuos hidrobiológicos para consumo animal SALMONOIL S.A. 1|",
        "20 01 39 | Plásticos mixtos 1450 kg Pretratamiento de plásticos REPLACAR 1|",
        "15 01 06 | Residuos mixtos 3400 kg Residuos municipales asimilables a domiciliarios Estación de transferencia 1|",
        "15 01 06 | Residuos mixtos 900 kg Disposición final ECOBIO 1|",
        "21 07 09 | Biosólidos 700 kg Compostaje Cancha compostaje Los Rebalses del Sur 1|",
    ]
    blocks = _reconstruct_row_blocks_from_lines(sample_lines)
    parsed = [_parse_reconstructed_row_block(b, STRONG_TREATMENT_CATALOG) for b in blocks]
    parsed = [p for p in parsed if p]
    p_by_code = {p["Código principal"]: p for p in parsed}
    return {
        "multiline_row_reconstructed": len(parsed) >= 5 and p_by_code.get("02 02 04", {}).get("Tratamiento") == "Degradación Anaeróbica",
        "code_desc_qty_split": bool(parsed and parsed[0]["Descripción Residuo"] and parsed[0]["Cantidad (Kg)"]),
        "cross_page_like_continuity": len(blocks) >= 5,
        "header_line_filtered": all("residuo cantidad" not in _norm(p.get("Texto fila original", "")) for p in parsed),
        "known_cases_treatment_destination": (
            p_by_code.get("19 08 05", {}).get("Tratamiento") == "Recepción de Lodos en PTAS"
            and "PLANTA DE TRATAMIENTO" in p_by_code.get("19 08 05", {}).get("Destino", "")
            and p_by_code.get("15 01 01", {}).get("Tratamiento") == "Reciclaje de papel, cartón y productos de papel"
            and "ECOFIBRAS SUCURSAL PUERTO MONTT" in p_by_code.get("15 01 01", {}).get("Destino", "")
            and p_by_code.get("10 01 01", {}).get("Tratamiento") == "Sitio de Escombros de la Construcción"
            and "ESCOMBRERA TRESOL" in p_by_code.get("10 01 01", {}).get("Destino", "")
            and p_by_code.get("20 01 99", {}).get("Tratamiento") == "Relleno sanitario"
            and "CONSORCIO COLLIPULLI" in p_by_code.get("20 01 99", {}).get("Destino", "")
            and p_by_code.get("21 04 04", {}).get("Destino") == "PLASTICOS DEL SUR SPA"
            and _norm(p_by_code.get("02 01 99", {}).get("Destino", "")) == _norm("Centro Crucero")
            and p_by_code.get("02 02 03", {}).get("Tratamiento") == "Reciclaje de residuos hidrobiológicos para consumo animal"
            and p_by_code.get("20 01 39", {}).get("Destino") == "REPLACAR"
            and _norm(p_by_code.get("15 01 06", {}).get("Destino", "")) in {_norm("Estación de transferencia"), _norm("ECOBIO")}
            and _norm(p_by_code.get("21 07 09", {}).get("Destino", "")) in {
                _norm("CANCHA COMPOSTAJE LOS REBALSES DEL SUR"),
                _norm("CANCHA LOS REBALSES DEL SUR"),
            }
        ),
        "confidence_flags_working": (
            p_by_code.get("19 08 05", {}).get("Tratamiento_confiable") == "SI"
            and p_by_code.get("19 08 05", {}).get("Destino_confiable") == "SI"
            and p_by_code.get("15 01 01", {}).get("Tratamiento_confiable") == "SI"
            and p_by_code.get("10 01 01", {}).get("Destino_confiable") == "SI"
        ),
    }
