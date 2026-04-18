import re
import unicodedata
import logging
import os
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


def parse_sinader_rows_from_tables(pdf_path: str) -> List[Dict[str, str]]:
    rows_out: List[Dict[str, str]] = []

    def _merge_continuations(tb_rows, idx_res, idx_qty, idx_trt, idx_dst, idx_trp, idx_pat):
        merged = []
        for r in tb_rows:
            r = list(r) if r else []
            max_idx = max([i for i in [idx_res, idx_qty, idx_trt, idx_dst, idx_trp, idx_pat] if i is not None] + [0])
            if len(r) <= max_idx:
                r += [""] * (max_idx + 1 - len(r))
            if not merged:
                merged.append(r)
                continue
            res_val = _clean_cell(r[idx_res]) if idx_res is not None else ""
            qty_val = _clean_cell(r[idx_qty]) if idx_qty is not None else ""
            trt_val = _clean_cell(r[idx_trt]) if idx_trt is not None else ""
            dst_val = _clean_cell(r[idx_dst]) if idx_dst is not None else ""
            trp_val = _clean_cell(r[idx_trp]) if idx_trp is not None else ""
            pat_val = _clean_cell(r[idx_pat]) if idx_pat is not None else ""
            looks_like_continuation = (qty_val == "") and any([res_val, trt_val, dst_val, trp_val, pat_val])
            if looks_like_continuation:
                prev = merged[-1]
                if len(prev) <= max_idx:
                    prev += [""] * (max_idx + 1 - len(prev))
                    merged[-1] = prev
                def append_col(i: int, new_text: str):
                    if new_text:
                        old = _clean_cell(prev[i])
                        prev[i] = (old + " " + new_text).strip() if old else new_text
                append_col(idx_res, res_val)
                if idx_trt is not None:
                    append_col(idx_trt, trt_val)
                if idx_dst is not None:
                    append_col(idx_dst, dst_val)
                if idx_trp is not None:
                    append_col(idx_trp, trp_val)
                if idx_pat is not None:
                    append_col(idx_pat, pat_val)
                continue
            merged.append(r)
        return merged

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            try:
                tables = page.extract_tables(table_settings=STREAM_SETTINGS) or []
            except Exception:
                tables = page.extract_tables() or []
            for tb in tables:
                if not tb or not tb[0]:
                    continue
                header = [_norm(_clean_cell(c)) for c in tb[0]]
                header_join = " ".join(header)
                if not (("residuo" in header_join) and ("cantidad" in header_join) and ("destino" in header_join)):
                    continue

                def find_col(*needles: str) -> Optional[int]:
                    for i, h in enumerate(header):
                        if any(n in h for n in needles):
                            return i
                    return None

                c_res = find_col("residuo")
                c_qty = find_col("cantidad")
                c_trt = find_col("tratamiento", "tipo tratamiento", "tipo", "tipo anotado", "tipo anotado expandido")
                c_dst = find_col("destino")
                c_trp = find_col("transportista")
                c_pat = find_col("patente")
                if c_res is None or c_qty is None:
                    continue
                body_merged = _merge_continuations(tb[1:], c_res, c_qty, c_trt, c_dst, c_trp, c_pat)
                for r in body_merged:
                    def g(i):
                        if i is None or i >= len(r):
                            return ""
                        return _cell_join_multiline(r[i])
                    residuo_cell = g(c_res)
                    qty_cell = g(c_qty)
                    if _clean_cell(residuo_cell) == "" and _clean_cell(qty_cell) == "":
                        continue
                    cod, desc = _split_code_and_desc(residuo_cell)
                    rows_out.append({
                        "Código principal": cod,
                        "Descripción Residuo": desc,
                        "Cantidad (Kg)": qty_cell,
                        "Tratamiento": g(c_trt),
                        "Destino": g(c_dst),
                        "Transportista": g(c_trp),
                        "Patente": g(c_pat),
                        "Peligrosidad": "",
                        "Estado contenedor": "",
                        "Contenedor": "",
                    })
    uniq = {}
    for r in rows_out:
        key = (r.get("Código principal", ""), _norm(r.get("Descripción Residuo", "")), _clean_cell(r.get("Cantidad (Kg)", "")))
        uniq.setdefault(key, r)
    return list(uniq.values())


def parse_sinader_rows_from_text(full_text: str) -> List[Dict[str, str]]:
    rows_out = []
    if not full_text:
        return rows_out
    text_flat = full_text.replace("\r", "\n").replace("\u00a0", " ")
    text_flat = re.sub(r"\n+", "\n", text_flat)
    pattern = re.compile(
        r"(?P<codigo>\d{2}\s\d{2}\s\d{2})\s*\|\s*(?P<body>.*?)(?=(\d{2}\s\d{2}\s\d{2}\s*\|)|\Z)",
        flags=re.IGNORECASE | re.DOTALL,
    )

    def _extract_labeled_value(block: str, labels: List[str]) -> str:
        joined_labels = "|".join(re.escape(lbl) for lbl in labels)
        next_labels = r"(tipo\s*tratamiento|tratamiento|destino|transportista|patente|cantidad|$)"
        m = re.search(
            rf"(?:{joined_labels})\s*:\s*(.+?)(?=\s*(?:{next_labels})\s*:|\s*$)",
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        return _clean_cell(_cell_join_multiline(m.group(1))) if m else ""

    def _extract_qty(block: str) -> str:
        m = re.search(r"(\d[\d\.,]*)\s*kg\b", block, flags=re.IGNORECASE)
        return _clean_cell(m.group(1)) if m else ""

    for m in pattern.finditer(text_flat):
        body = _cell_join_multiline(m.group("body"))
        qty = _extract_qty(body)
        desc = re.sub(r"\b\d[\d\.,]*\s*kg\b.*$", "", body, flags=re.IGNORECASE).strip()
        desc = re.sub(r"^(Residuo\s+)?", "", desc, flags=re.IGNORECASE).strip()
        desc = re.sub(r"\b(Tipo Tratamiento|Tratamiento|Destino|Transportista|Patente)\b.*$", "", desc, flags=re.IGNORECASE).strip()
        if not desc:
            continue
        rows_out.append({
            "Código principal": _clean_cell(m.group("codigo")),
            "Descripción Residuo": desc,
            "Cantidad (Kg)": qty,
            "Tratamiento": _extract_labeled_value(body, ["Tipo Tratamiento", "Tratamiento"]),
            "Destino": _extract_labeled_value(body, ["Destino"]),
            "Transportista": _extract_labeled_value(body, ["Transportista"]),
            "Patente": _extract_labeled_value(body, ["Patente"]),
            "Peligrosidad": "",
            "Estado contenedor": "",
            "Contenedor": "",
        })
    uniq = {}
    for r in rows_out:
        key = (r.get("Código principal", ""), _norm(r.get("Descripción Residuo", "")), _clean_cell(r.get("Cantidad (Kg)", "")))
        uniq.setdefault(key, r)
    return list(uniq.values())


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
        }], meta
    detail_rows = parse_sinader_rows_from_tables(pdf_path) or parse_sinader_rows_from_text(full_text)
    out_rows = []
    for i, r in enumerate(detail_rows, start=1):
        out_rows.append({
            "N.": str(i),
            "Descripción Residuo": r.get("Descripción Residuo", ""),
            "Código principal": r.get("Código principal", ""),
            "Peligrosidad": r.get("Peligrosidad", ""),
            "Cantidad (Kg)": r.get("Cantidad (Kg)", ""),
            "Estado contenedor": r.get("Estado contenedor", ""),
            "Contenedor": r.get("Contenedor", ""),
            "Tratamiento": r.get("Tratamiento", ""),
            "Destino": r.get("Destino", ""),
            "Transportista": r.get("Transportista", ""),
            "Patente": r.get("Patente", ""),
            "Sin movimientos": "NO",
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
        "Contenedor", "Estado contenedor", "DEFRA",
    ]
    cols = [c for c in preferred_cols if c in df.columns] + [c for c in df.columns if c not in preferred_cols]
    df = df[cols] if not df.empty else pd.DataFrame(columns=preferred_cols)
    if "Cantidad (Kg)" in df.columns:
        df["Cantidad (Kg)"] = df["Cantidad (Kg)"].apply(_to_float_kg)
    catalog = load_residuo_catalog()
    df = apply_residuo_dictionary_correction(df, catalog)
    treatment_defra_map = load_treatment_defra_map()
    if "DEFRA" not in df.columns:
        df["DEFRA"] = ""
    df["DEFRA"] = df.apply(
        lambda r: defra_classification(
            desc_residuo=r.get("Descripción Residuo", ""),
            sin_movimientos=r.get("Sin movimientos", ""),
            codigo_principal=r.get("Código principal", ""),
            tratamiento=r.get("Tratamiento", ""),
            destino=r.get("Destino", ""),
        ),
        axis=1,
    )
    if "Tratamiento" in df.columns:
        df["DEFRA"] = df.apply(
            lambda r: map_treatment_to_defra(r.get("Tratamiento", ""), treatment_defra_map) or r.get("DEFRA", ""),
            axis=1,
        )
    Path(output_excel).parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_excel, index=False)
    logger.info("Excel generado: %s | filas=%s", output_excel, len(df))
    return df
