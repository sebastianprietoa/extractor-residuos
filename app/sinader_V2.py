import re
from pathlib import Path
import pdfplumber
import pandas as pd


# =========================
# CONFIGURACIÓN
# =========================

CARPETA_PDFS = r"D:\GREEN_TICKET\Vitapro\2025\Huella de carbono\Chile\Residuos\Resumen año SINADER\Resumen año"

TRATAMIENTOS_CONOCIDOS = [
    "Reciclaje de papel, cartón y productos de papel",
    "Residuos municipales asimilables a domiciliarios",
    "Sitio de Escombros de la Construcción",
    "Recepción de Lodos en PTAS",
    "Degradación Anaeróbica",
    "Reciclaje de plásticos",
    "Reciclaje de metales",
    "Disposición final",
    "Relleno sanitario",
    "Pretratamiento",
    "Monorelleno",
    "Compostaje",
]
TRATAMIENTOS_CONOCIDOS = sorted(TRATAMIENTOS_CONOCIDOS, key=len, reverse=True)

MAPA_RESIDUOS_SINADER = {
    "02 01 99": "Residuos no especificados en otra categoría",
    "02 02 04": "Lodos del tratamiento in situ de efluentes",
    "10 01 01": "Cenizas del hogar, escorias y polvo de caldera (excepto el polvo de caldera especificado en el código 10 01 04)",
    "15 01 01": "Envases de papel y cartón",
    "15 01 02": "Envases de plástico",
    "15 01 04": "Envases metálicos",
    "15 01 06": "Envases mezclados",
    "19 08 05": "Lodos del tratamiento de aguas residuales urbanas",
    "20 01 99": "Otras fracciones no especificadas en otra categoría",
    "21 04 04": "Residuos de plásticos (HDPE, PEE, PETE, PVC) excepto planzas, boyas, flotadores, redes y cabos",
    "21 07 01": "Residuos orgánicos (ejemplo como conchas, algas, carne, entre otros; incluye mortalidad)",
    "21 07 09": "Lodos orgánicos (ejemplo fecas y alimento no consumido)",
}

REGEX_CODIGO_INICIO = re.compile(r"^\s*(\d{2}\s\d{2}\s\d{2})\s*\|")
REGEX_CANTIDAD = re.compile(r"(\d+(?:[.,]\d+)?)\s*kg\b", re.IGNORECASE)

FIN_TABLA_PATTERNS = [
    "La integridad y veracidad de la información",
    "DECLARACIÓN MENSUAL DE RESIDUOS NO PELIGROSOS",
    "Documento generado electrónicamente",
]


# =========================
# EXTRACCIÓN POR COORDENADAS
# =========================

def extraer_lineas_por_coordenadas(pdf_path: Path) -> list[str]:
    lineas_totales = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            words = page.extract_words(
                x_tolerance=2,
                y_tolerance=3,
                keep_blank_chars=False,
                use_text_flow=False,
            )

            if not words:
                continue

            grupos = {}
            for w in words:
                y = round(w["top"], 1)
                grupos.setdefault(y, []).append(w)

            for y in sorted(grupos.keys()):
                fila = sorted(grupos[y], key=lambda z: z["x0"])
                texto_linea = " ".join(w["text"] for w in fila)
                texto_linea = re.sub(r"\s+", " ", texto_linea).strip()
                if texto_linea:
                    lineas_totales.append(texto_linea)

    return lineas_totales


def extraer_texto_general_desde_lineas(lineas: list[str]) -> str:
    return "\n".join(lineas)


# =========================
# METADATOS
# =========================

def extraer_metadatos(texto: str, pdf_path: Path) -> dict:
    def buscar(patron: str):
        m = re.search(patron, texto, flags=re.IGNORECASE)
        return m.group(1).strip() if m else None

    return {
        "archivo": pdf_path.name,
        "folio": buscar(r"N°\s*FOLIO:\s*[_\s]*([0-9]+)"),
        "establecimiento": buscar(r"ESTABLECIMIENTO:\s*[_\s]*(.+)"),
        "periodo_declarado": buscar(r"PERIODO\s+DECLARADO:\s*([A-Za-zÁÉÍÓÚáéíóúñÑ]+\s*-\s*\d{4})"),
        "fecha_declaracion": buscar(r"FECHA\s+DECLARACIÓN:\s*([0-9]{2}-[0-9]{2}-[0-9]{4})"),
    }


# =========================
# AISLAR BLOQUE DE TABLA
# =========================

def aislar_bloque_tabla_desde_lineas(lineas: list[str]) -> list[str]:
    inicio = None
    fin = None

    for i, linea in enumerate(lineas):
        linea_norm = re.sub(r"\s+", " ", linea).strip().lower()
        if (
            "residuo" in linea_norm
            and "cantidad" in linea_norm
            and "tratamiento" in linea_norm
            and "destino" in linea_norm
        ):
            inicio = i + 1
            break

    if inicio is None:
        for i, linea in enumerate(lineas):
            if re.match(r"^\d{2}\s\d{2}\s\d{2}\s*\|", linea):
                inicio = i
                break

    if inicio is None:
        return []

    for j in range(inicio, len(lineas)):
        if any(p.lower() in lineas[j].lower() for p in FIN_TABLA_PATTERNS):
            fin = j
            break

    if fin is None:
        fin = len(lineas)

    return lineas[inicio:fin]


def es_periodo_sin_movimientos_lineas(lineas_tabla: list[str]) -> bool:
    texto = " ".join(lineas_tabla)
    return "Período Sin Movimientos" in texto or "Periodo Sin Movimientos" in texto


# =========================
# RECONSTRUCCIÓN DE FILAS
# =========================

def reconstruir_filas_desde_lineas(lineas_tabla: list[str]) -> list[str]:
    filas = []
    actual = ""

    for linea in lineas_tabla:
        linea = re.sub(r"\s+", " ", linea).strip()
        if not linea:
            continue

        if re.match(r"^\d{2}\s\d{2}\s\d{2}\s*\|", linea):
            if actual:
                filas.append(actual.strip())
            actual = linea
        else:
            if actual:
                actual += " " + linea

    if actual:
        filas.append(actual.strip())

    return filas


# =========================
# NORMALIZACIÓN DE FILAS
# =========================

def normalizar_fila_original(fila: str) -> str:
    fila = re.sub(r"\s+", " ", fila).strip()

    fila = fila.replace("ECOFIBRASSUCURSAL", "ECOFIBRAS SUCURSAL")
    fila = fila.replace("PUERTOMONTT", "PUERTO MONTT")
    fila = fila.replace("ECOFIBRASSUCURSALPUERTOMONTT", "ECOFIBRAS SUCURSAL PUERTO MONTT")

    fila = fila.replace("carton", "cartón")
    fila = fila.replace("anaerobica", "anaeróbica")
    fila = fila.replace("construccion", "construcción")
    fila = fila.replace("disposicion final", "disposición final")
    fila = fila.replace("recepcion de lodos en ptas", "recepción de lodos en ptas")

    fila = fila.replace("PTA", "PTAS")
    fila = fila.replace("Recepción de Lodos en PTA", "Recepción de Lodos en PTAS")
    fila = fila.replace("Recepcion de Lodos en PTA", "Recepción de Lodos en PTAS")

    return fila


# =========================
# TRATAMIENTOS
# =========================

def encontrar_tratamiento_en_texto(texto: str):
    texto_norm = re.sub(r"\s+", " ", texto).strip().lower()

    for t in TRATAMIENTOS_CONOCIDOS:
        if t.lower() in texto_norm:
            return t

    if "degradación" in texto_norm and "anaeróbica" in texto_norm:
        return "Degradación Anaeróbica"

    if "sitio de escombros" in texto_norm and "construcción" in texto_norm:
        return "Sitio de Escombros de la Construcción"

    if "monorelleno" in texto_norm:
        return "Monorelleno"

    if "compostaje" in texto_norm:
        return "Compostaje"

    if "pretratamiento" in texto_norm:
        return "Pretratamiento"

    if "relleno sanitario" in texto_norm:
        return "Relleno sanitario"

    if "disposición final" in texto_norm or "disposicion final" in texto_norm:
        return "Disposición final"

    if "reciclaje de plásticos" in texto_norm or "reciclaje de plasticos" in texto_norm:
        return "Reciclaje de plásticos"

    if "reciclaje de metales" in texto_norm:
        return "Reciclaje de metales"

    if "recepción de lodos en ptas" in texto_norm or "recepcion de lodos en ptas" in texto_norm:
        return "Recepción de Lodos en PTAS"

    if "residuos municipales" in texto_norm and "asimilables a domiciliarios" in texto_norm:
        return "Residuos municipales asimilables a domiciliarios"

    return None


def inferir_tratamiento_por_codigo(codigo: str, fila_completa: str = "", resto_post_cantidad: str = ""):
    fila_norm = re.sub(r"\s+", " ", fila_completa).strip().lower()
    resto_norm = re.sub(r"\s+", " ", resto_post_cantidad).strip().lower()
    combinado = f"{fila_norm} {resto_norm}"

    if codigo == "15 01 02":
        if (
            "reciclaje de plásticos" in combinado
            or "reciclaje de plasticos" in combinado
            or "plástico" in fila_norm
            or "plastico" in fila_norm
        ):
            return "Reciclaje de plásticos"

    if codigo == "15 01 04":
        if (
            "reciclaje de metales" in combinado
            or "metálico" in fila_norm
            or "metalico" in fila_norm
            or "metales" in combinado
        ):
            return "Reciclaje de metales"

    if codigo == "19 08 05":
        if (
            "recepción de lodos" in combinado
            or "recepcion de lodos" in combinado
            or "planta de tratamiento" in combinado
            or "aguas servidas" in combinado
            or "tratamiento de aguas" in combinado
            or "ptas" in combinado
        ):
            return "Recepción de Lodos en PTAS"

    return None


# =========================
# PARSERS
# =========================

def parsear_fila_metodo_1(fila: str):
    fila = normalizar_fila_original(fila)
    fila = re.sub(r"\s+", " ", fila).strip()

    m_codigo = re.match(r"^(\d{2}\s\d{2}\s\d{2})\s*\|\s*", fila)
    if not m_codigo:
        return None

    codigo = m_codigo.group(1).strip()
    resto = fila[m_codigo.end():].strip()

    m_cantidad = REGEX_CANTIDAD.search(resto)
    if not m_cantidad:
        return None

    cantidad_str = m_cantidad.group(1).replace(",", ".")
    cantidad = float(cantidad_str)

    residuo = resto[:m_cantidad.start()].strip()
    residuo = re.sub(r"\s+", " ", residuo).strip(" -|,")

    resto_post = resto[m_cantidad.end():].strip()
    tratamiento = encontrar_tratamiento_en_texto(resto_post)

    if tratamiento is None:
        tratamiento = inferir_tratamiento_por_codigo(
            codigo=codigo,
            fila_completa=fila,
            resto_post_cantidad=resto_post,
        )

    if tratamiento is None and codigo == "15 01 01":
        resto_post_norm = re.sub(r"\s+", " ", resto_post).lower()
        tiene_reciclaje = "reciclaje" in resto_post_norm
        tiene_papel = "papel" in resto_post_norm
        tiene_carton = ("cartón" in resto_post_norm) or ("carton" in resto_post_norm)
        tiene_productos = ("productos" in resto_post_norm) or ("productos de" in resto_post_norm)

        if tiene_reciclaje or (tiene_papel and tiene_carton and tiene_productos):
            tratamiento = "Reciclaje de papel, cartón y productos de papel"

    return {
        "codigo_residuo": codigo,
        "residuo": residuo,
        "cantidad_kg": cantidad,
        "tratamiento": tratamiento,
        "fila_original": fila,
        "metodo_usado": "metodo_1",
    }


def parsear_fila_metodo_2_rescate(fila: str):
    fila = normalizar_fila_original(fila)
    fila = re.sub(r"\s+", " ", fila).strip()

    m_codigo = re.match(r"^(\d{2}\s\d{2}\s\d{2})\s*\|\s*", fila)
    if not m_codigo:
        return None

    codigo = m_codigo.group(1).strip()
    resto = fila[m_codigo.end():].strip()

    m_cantidad = REGEX_CANTIDAD.search(resto)
    if not m_cantidad:
        return None

    cantidad_str = m_cantidad.group(1).replace(",", ".")
    cantidad = float(cantidad_str)

    residuo = resto[:m_cantidad.start()].strip()
    residuo = re.sub(r"\s+", " ", residuo).strip(" -|,")

    resto_post = resto[m_cantidad.end():].strip()
    tratamiento = encontrar_tratamiento_en_texto(fila)

    if tratamiento is None:
        tratamiento = inferir_tratamiento_por_codigo(
            codigo=codigo,
            fila_completa=fila,
            resto_post_cantidad=resto_post,
        )

    if tratamiento is None and codigo == "15 01 01":
        fila_norm = re.sub(r"\s+", " ", fila).lower()
        resto_post_norm = re.sub(r"\s+", " ", resto_post).lower()

        tiene_reciclaje = "reciclaje" in fila_norm or "reciclaje" in resto_post_norm
        tiene_papel = "papel" in fila_norm or "papel" in resto_post_norm
        tiene_carton = (
            "cartón" in fila_norm or "carton" in fila_norm
            or "cartón" in resto_post_norm or "carton" in resto_post_norm
        )
        tiene_productos = (
            "productos" in fila_norm or "productos de" in fila_norm
            or "productos" in resto_post_norm or "productos de" in resto_post_norm
        )

        if tiene_reciclaje or (tiene_papel and tiene_carton and tiene_productos):
            tratamiento = "Reciclaje de papel, cartón y productos de papel"

    if tratamiento:
        residuo = re.sub(re.escape(tratamiento), "", residuo, flags=re.IGNORECASE).strip(" -|,")

    return {
        "codigo_residuo": codigo,
        "residuo": residuo,
        "cantidad_kg": cantidad,
        "tratamiento": tratamiento,
        "fila_original": fila,
        "metodo_usado": "metodo_2_rescate",
    }


def parsear_fila(fila: str):
    resultado_1 = parsear_fila_metodo_1(fila)

    if resultado_1 is None:
        return None

    if resultado_1["tratamiento"] is not None:
        resultado_1["requiere_revision"] = False
        return resultado_1

    resultado_2 = parsear_fila_metodo_2_rescate(fila)

    if resultado_2 is not None and resultado_2["tratamiento"] is not None:
        resultado_2["requiere_revision"] = True
        return resultado_2

    resultado_1["requiere_revision"] = True
    return resultado_1


# =========================
# PROCESAMIENTO PDF
# =========================

def procesar_pdf(pdf_path: Path) -> list[dict]:
    lineas = extraer_lineas_por_coordenadas(pdf_path)
    texto_general = extraer_texto_general_desde_lineas(lineas)
    meta = extraer_metadatos(texto_general, pdf_path)

    bloque_tabla = aislar_bloque_tabla_desde_lineas(lineas)

    if not bloque_tabla:
        return [{
            **meta,
            "sin_movimientos": None,
            "codigo_residuo": None,
            "residuo": None,
            "cantidad_kg": None,
            "tratamiento": None,
            "fila_original": None,
            "metodo_usado": None,
            "requiere_revision": True,
            "observacion": "No se encontró tabla",
        }]

    if es_periodo_sin_movimientos_lineas(bloque_tabla):
        return [{
            **meta,
            "sin_movimientos": True,
            "codigo_residuo": None,
            "residuo": None,
            "cantidad_kg": None,
            "tratamiento": None,
            "fila_original": None,
            "metodo_usado": None,
            "requiere_revision": False,
            "observacion": "Período Sin Movimientos",
        }]

    filas = reconstruir_filas_desde_lineas(bloque_tabla)

    resultados = []
    for fila in filas:
        parsed = parsear_fila(fila)

        if parsed is None:
            resultados.append({
                **meta,
                "sin_movimientos": False,
                "codigo_residuo": None,
                "residuo": None,
                "cantidad_kg": None,
                "tratamiento": None,
                "fila_original": fila,
                "metodo_usado": None,
                "requiere_revision": True,
                "observacion": "Fila no parseada",
            })
        else:
            resultados.append({
                **meta,
                "sin_movimientos": False,
                **parsed,
                "observacion": None,
            })

    return resultados


# =========================
# NORMALIZAR RESIDUOS POR CÓDIGO
# =========================

def normalizar_residuos_por_codigo(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["residuo_extraido"] = df["residuo"]
    df["residuo_oficial"] = df["codigo_residuo"].map(MAPA_RESIDUOS_SINADER)
    df["residuo"] = df["residuo_oficial"].combine_first(df["residuo"])
    df["codigo_sin_mapa_residuo"] = df["codigo_residuo"].notna() & df["residuo_oficial"].isna()
    return df


# =========================
# PROCESAMIENTO MASIVO
# =========================

def procesar_carpeta(carpeta_pdfs: str):
    carpeta = Path(carpeta_pdfs)
    pdfs = sorted(carpeta.rglob("*.pdf"))

    print(f"PDFs encontrados: {len(pdfs)}")

    todos = []

    for pdf in pdfs:
        try:
            todos.extend(procesar_pdf(pdf))
            print("OK:", pdf.name)
        except Exception as e:
            todos.append({
                "archivo": pdf.name,
                "folio": None,
                "establecimiento": None,
                "periodo_declarado": None,
                "fecha_declaracion": None,
                "sin_movimientos": None,
                "codigo_residuo": None,
                "residuo": None,
                "cantidad_kg": None,
                "tratamiento": None,
                "fila_original": None,
                "metodo_usado": None,
                "requiere_revision": True,
                "observacion": f"Error: {e}",
            })
            print("ERROR:", pdf.name, e)

    df = pd.DataFrame(todos)

    columnas = [
        "archivo",
        "folio",
        "establecimiento",
        "periodo_declarado",
        "fecha_declaracion",
        "sin_movimientos",
        "codigo_residuo",
        "residuo",
        "cantidad_kg",
        "tratamiento",
        "metodo_usado",
        "requiere_revision",
        "observacion",
        "fila_original",
    ]

    df = df[columnas]
    df = normalizar_residuos_por_codigo(df)

    return df


# =========================
# EXPORTACIÓN
# =========================

def exportar_resultados(df: pd.DataFrame, carpeta_pdfs: str):
    carpeta = Path(carpeta_pdfs)

    df_ok = df[(df["sin_movimientos"] == False) & (df["codigo_residuo"].notna())].copy()
    df_revision = df[
        (df["observacion"].notna())
        | (df["tratamiento"].isna())
        | (df["requiere_revision"] == True)
        | (df["codigo_sin_mapa_residuo"] == True)
    ].copy()

    salida = carpeta / "extraccion_residuos_sinader_metodo_doble.xlsx"

    columnas_exportar = [
        "archivo",
        "folio",
        "establecimiento",
        "periodo_declarado",
        "fecha_declaracion",
        "sin_movimientos",
        "codigo_residuo",
        "residuo",
        "residuo_extraido",
        "residuo_oficial",
        "cantidad_kg",
        "tratamiento",
        "metodo_usado",
        "requiere_revision",
        "codigo_sin_mapa_residuo",
        "observacion",
        "fila_original",
    ]

    with pd.ExcelWriter(salida, engine="openpyxl") as writer:
        df[columnas_exportar].to_excel(writer, sheet_name="Completo", index=False)
        df_ok[columnas_exportar].to_excel(writer, sheet_name="OK", index=False)
        df_revision[columnas_exportar].to_excel(writer, sheet_name="Revision", index=False)

    print("Generado:", salida)
    return salida


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    df = procesar_carpeta(CARPETA_PDFS)
    print(df.head(20))
    exportar_resultados(df, CARPETA_PDFS)