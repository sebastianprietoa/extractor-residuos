
import re
from pathlib import Path
import tkinter as tk
from tkinter import filedialog

import pdfplumber
import pandas as pd

OUTPUT_NAME = "salida_consolidada_muestras_y_caudales_corregida_v5_2.xlsx"


def escoger_carpeta_input() -> Path:
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    carpeta = filedialog.askdirectory(title="Selecciona la carpeta con los certificados PDF")
    if not carpeta:
        raise SystemExit("No seleccionaste ninguna carpeta. Proceso cancelado.")
    return Path(carpeta)


def limpiar_celda(celda):
    if celda is None:
        return ""
    return str(celda).replace("\n", " ").strip()


def normalizar_texto(s: str) -> str:
    s = (s or "").replace("\xa0", " ").strip()
    return re.sub(r"\s+", " ", s)


def extraer_centro_desde_nombre_archivo(path: Path) -> str:
    nombre = path.stem
    nombre = re.sub(r"\s*\(\d+\)$", "", nombre)
    return nombre.strip()


def add_unique(registros, seen, registro):
    key = (
        registro.get("archivo", ""),
        registro.get("numero_muestra", ""),
        registro.get("tipo_registro", ""),
        registro.get("parametro", ""),
        registro.get("unidad_medida", ""),
        registro.get("valor_limite", ""),
        registro.get("valor_medido", ""),
        registro.get("numero_fila_caudal", ""),
        registro.get("fecha_caudal", ""),
        registro.get("hora_caudal", ""),
    )
    if key in seen:
        return
    seen.add(key)
    registros.append(registro)


# ------------------------------
# NUMÉRICOS
# ------------------------------

def limpiar_valor_a_numero(valor):
    """
    Convierte textos como:
    '<0,5' -> 0.5
    '3555 m3/dia' -> 3555.0
    '34.555 m3/dia' -> 34555.0
    '1.234,56 mg/L' -> 1234.56
    '-' -> None
    """
    if valor is None:
        return None

    s = normalizar_texto(str(valor))
    if s in {"", "-", "—"}:
        return None

    s = re.sub(r"^[<>]=?\s*", "", s)

    # Extraer tramo numérico principal
    m = re.search(r"\d[\d\.,]*", s)
    if not m:
        return None

    num = m.group(0)

    # Reglas robustas:
    # 1) si tiene . y , => . miles, , decimal
    # 2) si solo tiene , => , decimal
    # 3) si solo tiene .:
    #    - si termina en .ddd => es miles
    #    - si no => decimal
    if "." in num and "," in num:
        num = num.replace(".", "").replace(",", ".")
    elif "," in num:
        num = num.replace(",", ".")
    elif "." in num:
        # uno o varios grupos de miles
        if re.fullmatch(r"\d{1,3}(?:\.\d{3})+", num):
            num = num.replace(".", "")
        # decimal con punto: se deja tal cual
        else:
            pass

    try:
        return float(num)
    except ValueError:
        return None


def limpiar_columna_valor_medido(df: pd.DataFrame) -> pd.DataFrame:
    if "valor_medido" in df.columns:
        df["valor_medido"] = df["valor_medido"].apply(limpiar_valor_a_numero)
    return df


def limpiar_columna_caudal_informado(df: pd.DataFrame) -> pd.DataFrame:
    if "caudal_informado" in df.columns:
        df["caudal_informado_num"] = df["caudal_informado"].apply(limpiar_valor_a_numero)
        df["caudal_informado"] = df["caudal_informado_num"]
    else:
        df["caudal_informado_num"] = pd.NA
        df["caudal_informado"] = pd.NA
    return df


def limpiar_valor_limite_texto(s):
    if s is None:
        return ""
    return normalizar_texto(str(s))


# ------------------------------
# CAMPOS GENERALES
# ------------------------------

def buscar_campo(texto: str, etiqueta: str) -> str:
    patron = rf"{re.escape(etiqueta)}\s*:\s*([^\n\r]+)"
    m = re.search(patron, texto, re.IGNORECASE)
    return m.group(1).strip() if m else ""


def buscar_entre_etiquetas(texto: str, etiqueta_ini: str, etiqueta_fin: str) -> str:
    patron = rf"{re.escape(etiqueta_ini)}\s*:\s*(.*?)\s+(?={re.escape(etiqueta_fin)}\s*:)"
    m = re.search(patron, texto, re.IGNORECASE | re.DOTALL)
    return normalizar_texto(m.group(1)) if m else ""


def extraer_datos_generales(texto: str) -> dict:
    return {
        "folio": buscar_entre_etiquetas(texto, "Folio", "Fecha de Ingreso al Sistema") or buscar_campo(texto, "Folio"),
        "empresa": buscar_campo(texto, "Empresa"),
        "establecimiento": buscar_campo(texto, "Establecimiento"),
        "ducto": buscar_campo(texto, "Ducto"),
        "tipo_control": buscar_entre_etiquetas(texto, "Tipo Control", "Período de Evaluación") or buscar_campo(texto, "Tipo Control"),
        "periodo": buscar_campo(texto, "Período de Evaluación") or buscar_campo(texto, "Periodo de Evaluación"),
    }


# ------------------------------
# MUESTRAS
# ------------------------------

def extraer_bloques_muestra(texto_completo: str):
    patron = re.compile(
        r"(Muestra\s*(?:N[º°o]\s*)?(\d+).*?)(?=Muestra\s*(?:N[º°o]\s*)?\d+|Página\s+\d+\s+de\s+\d+|$)",
        re.IGNORECASE | re.DOTALL,
    )
    return [(bloque, num) for bloque, num in patron.findall(texto_completo)]


def extraer_datos_muestra(texto_muestra: str, numero_muestra: str) -> dict:
    def campo_inline(etq_a, etq_b=None):
        if etq_b:
            val = buscar_entre_etiquetas(texto_muestra, etq_a, etq_b)
            if val:
                return val
        return buscar_campo(texto_muestra, etq_a)

    laboratorio = (
        buscar_entre_etiquetas(texto_muestra, "ETFA", "Fecha de Ingreso Laboratorio")
        or buscar_campo(texto_muestra, "ETFA")
        or buscar_campo(texto_muestra, "Laboratorio")
        or buscar_campo(texto_muestra, "Nombre Laboratorio")
    )

    return {
        "numero_muestra": numero_muestra,
        "tipo_muestra": campo_inline("Tipo de Muestra", "Fecha de Muestreo"),
        "fecha_muestreo": buscar_campo(texto_muestra, "Fecha de Muestreo"),
        "hora_inicio_muestreo": campo_inline("Hora Inicio de Muestreo", "Hora Término de Muestreo")
                                or campo_inline("Hora Inicio", "Hora Término de Muestreo")
                                or campo_inline("Hora Inicio", "Hora Termino de Muestreo")
                                or buscar_campo(texto_muestra, "Hora Inicio de Muestreo")
                                or buscar_campo(texto_muestra, "Hora Inicio"),
        "hora_termino_muestreo": buscar_campo(texto_muestra, "Hora Término de Muestreo")
                                 or buscar_campo(texto_muestra, "Hora Termino de Muestreo"),
        "caudal_comprometido": campo_inline("Caudal Comprometido", "Caudal Informado")
                               or buscar_campo(texto_muestra, "Caudal Comprometido"),
        "caudal_informado": buscar_campo(texto_muestra, "Caudal Informado"),
        "laboratorio": laboratorio,
        "codigo_informe": buscar_campo(texto_muestra, "Código de Informe de Laboratorio")
                          or buscar_campo(texto_muestra, "Codigo de Informe de Laboratorio"),
    }


# ------------------------------
# PARSEO DE FILAS DE PARÁMETROS
# ------------------------------

UNITS_PATTERN = r"mgO2/L|mg/L|m3/dia|m3/día|NMP/100\s*ml|Unidad|mm|°C|°c"


def parsear_linea_parametro(linea: str):
    linea = normalizar_texto(linea)

    patron = rf"^(?P<param>.+?)\s+(?P<unidad>{UNITS_PATTERN})\s+(?P<rest>.+)$"
    m = re.match(patron, linea, re.IGNORECASE)
    if not m:
        return None

    parametro = normalizar_texto(m.group("param"))
    unidad = normalizar_texto(m.group("unidad"))
    rest = normalizar_texto(m.group("rest"))

    m2 = re.match(r"^(?P<limite>.+?)\s+(?P<medido>\S+)$", rest)
    if not m2:
        return None

    limite = normalizar_texto(m2.group("limite"))
    medido = normalizar_texto(m2.group("medido"))

    return {
        "parametro": parametro,
        "unidad_medida": unidad,
        "valor_limite": limite,
        "valor_medido": medido,
    }


def deduplicar_parametros_misma_muestra(parametros):
    vistos = set()
    out = []
    for p in parametros:
        key = (
            p.get("parametro", ""),
            p.get("unidad_medida", ""),
            p.get("valor_limite", ""),
            p.get("valor_medido", ""),
        )
        if key in vistos:
            continue
        vistos.add(key)
        out.append(p)
    return out


def debe_omitir_parametro_compuesto(parametro: str, tipo_muestra: str):
    p = normalizar_texto(parametro).lower()
    t = normalizar_texto(tipo_muestra).lower()
    return t == "compuesta" and p == "caudal"


# ------------------------------
# PUNTUALES POR TEXTO
# ------------------------------

def parsear_bloque_parametro_puntual(texto_bloque: str, nombre_parametro: str, unidad: str, limite: str):
    registros = []
    lineas = [normalizar_texto(x) for x in texto_bloque.splitlines() if normalizar_texto(x)]

    for linea in lineas:
        m1 = re.match(r"^(?P<n>\d+)\s+(?P<fecha>\d{2}/\d{2}/\d{4})\s+(?P<hora>\d{2}:\d{2})\s+(?P<valor>.+)$", linea)
        m2 = re.match(r"^(?P<n>\d+)\s+(?P<fecha>\d{2}/\d{2}/\d{4})\s+(?P<valor>.+)$", linea)

        if m1:
            n, fecha, hora, valor = m1.group("n"), m1.group("fecha"), m1.group("hora"), m1.group("valor").strip()
        elif m2:
            n, fecha, hora, valor = m2.group("n"), m2.group("fecha"), "", m2.group("valor").strip()
        else:
            continue

        registros.append({
            "tipo_registro": "caudal_diario" if nombre_parametro.lower() == "caudal" else "parametro_puntual",
            "parametro": "Caudal diario" if nombre_parametro.lower() == "caudal" else nombre_parametro,
            "unidad_medida": "m3/dia" if nombre_parametro.lower() == "caudal" else unidad,
            "valor_limite": "" if nombre_parametro.lower() == "caudal" else limite,
            "valor_medido": valor,
            "numero_fila_caudal": n,
            "fecha_caudal": fecha,
            "hora_caudal": hora,
        })

    return registros


def extraer_bloques_puntuales_desde_texto(texto_pagina: str):
    texto = texto_pagina or ""
    resultados = []

    patron = re.compile(
        r"(?P<param>Caudal|pH|Temperatura)\s+Unidad\s+de\s+Medida:\s*(?P<unidad>.*?)\s+L[íi]mite:\s*(?P<limite>.*?)\s+N[°º]\s+Fecha\s+Descarga\s+Valor\s+Medido\s*(?P<body>.*?)(?=(?:Caudal|pH|Temperatura)\s+Unidad\s+de\s+Medida:|Página\s+\d+\s+de\s+\d+|$)",
        re.IGNORECASE | re.DOTALL,
    )

    for m in patron.finditer(texto):
        resultados.extend(
            parsear_bloque_parametro_puntual(
                texto_bloque=m.group("body"),
                nombre_parametro=normalizar_texto(m.group("param")),
                unidad=normalizar_texto(m.group("unidad")),
                limite=normalizar_texto(m.group("limite")),
            )
        )

    return resultados


# ------------------------------
# PARSEO POR BLOQUE DE MUESTRA
# ------------------------------

def extraer_parametros_desde_bloque_texto(texto_muestra: str):
    registros = []

    lineas = [normalizar_texto(l) for l in texto_muestra.splitlines() if normalizar_texto(l)]

    inicio = None
    for i, l in enumerate(lineas):
        if "detalle parámetros reportados" in l.lower() or "detalle parametros reportados" in l.lower():
            inicio = i + 1
            break

    if inicio is None:
        return registros

    datos = lineas[inicio:]
    for linea in datos:
        if "parámetro" in linea.lower() or "parametro" in linea.lower():
            continue
        if re.match(r"^n[°º]\s+fecha\s+descarga", linea.lower()):
            break

        p = parsear_linea_parametro(linea)
        if p:
            registros.append(p)

    return deduplicar_parametros_misma_muestra(registros)


def parsear_pdf(path_pdf: Path):
    print(f"\nProcesando: {path_pdf.name}")
    registros = []
    seen = set()

    with pdfplumber.open(path_pdf) as pdf:
        textos_paginas = [page.extract_text() or "" for page in pdf.pages]
        texto_completo = "\n".join(textos_paginas)

        archivo = path_pdf.name
        centro = extraer_centro_desde_nombre_archivo(path_pdf)
        datos_generales = extraer_datos_generales(texto_completo)

        bloques = extraer_bloques_muestra(texto_completo)

        for bloque_texto, numero_muestra in bloques:
            datos_muestra = extraer_datos_muestra(bloque_texto, numero_muestra)
            tipo_muestra = datos_muestra.get("tipo_muestra", "")

            if normalizar_texto(tipo_muestra).lower() == "puntual":
                puntuales = extraer_bloques_puntuales_desde_texto(bloque_texto)
                for r in puntuales:
                    registro = {"archivo": archivo, "centro": centro, **r}
                    registro.update(datos_generales)
                    registro.update(datos_muestra)
                    add_unique(registros, seen, registro)
                continue

            params = extraer_parametros_desde_bloque_texto(bloque_texto)
            for p in params:
                if debe_omitir_parametro_compuesto(p["parametro"], tipo_muestra):
                    continue

                registro = {
                    "archivo": archivo,
                    "centro": centro,
                    "tipo_registro": "parametro",
                    "parametro": p["parametro"],
                    "unidad_medida": p["unidad_medida"],
                    "valor_limite": limpiar_valor_limite_texto(p["valor_limite"]),
                    "valor_medido": p["valor_medido"],
                    "numero_fila_caudal": "",
                    "fecha_caudal": "",
                    "hora_caudal": "",
                }
                registro.update(datos_generales)
                registro.update(datos_muestra)
                add_unique(registros, seen, registro)

    print(f"  Registros generados: {len(registros)}")
    return registros


# ------------------------------
# RESUMEN MENSUAL
# ------------------------------

def construir_resumen_mensual(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    df_work = df.copy()
    df_work["periodo"] = df_work["periodo"].astype(str).apply(normalizar_texto)
    df_work["tipo_muestra_norm"] = df_work["tipo_muestra"].astype(str).str.lower().str.strip()
    df_work["parametro_norm"] = df_work["parametro"].astype(str).apply(normalizar_texto)

    df_valid = df_work[df_work["valor_medido"].notna()].copy()

    medias = (
        df_valid.groupby(["periodo", "tipo_muestra_norm", "parametro_norm"], dropna=False)["valor_medido"]
        .mean()
        .reset_index()
    )

    medias_comp = medias[medias["tipo_muestra_norm"] == "compuesta"].copy()
    medias_punt = medias[medias["tipo_muestra_norm"] == "puntual"].copy()

    comp_map = {(r["periodo"], r["parametro_norm"]): r["valor_medido"] for _, r in medias_comp.iterrows()}
    punt_map = {(r["periodo"], r["parametro_norm"]): r["valor_medido"] for _, r in medias_punt.iterrows()}

    caudal_inf = (
        df_work[df_work["caudal_informado_num"].notna()]
        .groupby("periodo", dropna=False)["caudal_informado_num"]
        .mean()
    )
    caudal_inf_map = caudal_inf.to_dict()

    periodos = list(dict.fromkeys(df_work["periodo"].dropna().tolist()))
    parametros_comp = list(dict.fromkeys(medias_comp["parametro_norm"].tolist()))
    parametros_punt = list(dict.fromkeys(medias_punt["parametro_norm"].tolist()))

    parametros_finales = []
    for p in parametros_comp + parametros_punt:
        if p not in parametros_finales and p != "":
            parametros_finales.append(p)

    if "Caudal diario" not in parametros_finales and ("Caudal diario" in medias_punt["parametro_norm"].tolist() or len(caudal_inf_map) > 0):
        parametros_finales.append("Caudal diario")

    filas = []
    for periodo in periodos:
        row = {"periodo": periodo}

        for parametro in parametros_finales:
            if parametro == "Caudal diario":
                valor = punt_map.get((periodo, "Caudal diario"))
                if pd.isna(valor) or valor is None:
                    valor = caudal_inf_map.get(periodo)
            else:
                valor = comp_map.get((periodo, parametro))
                if pd.isna(valor) or valor is None:
                    valor = punt_map.get((periodo, parametro))

            row[parametro] = valor

        filas.append(row)

    resumen = pd.DataFrame(filas)
    cols = ["periodo"] + [c for c in parametros_finales if c in resumen.columns]
    return resumen.reindex(columns=cols)


def main():
    input_dir = escoger_carpeta_input()
    output_excel = input_dir / OUTPUT_NAME

    pdfs = sorted([f for f in input_dir.iterdir() if f.suffix.lower() == ".pdf"])
    print(f"\nCarpeta seleccionada: {input_dir}")
    print(f"PDF encontrados: {len(pdfs)}")

    todos_registros = []
    errores = []

    for file in pdfs:
        try:
            todos_registros.extend(parsear_pdf(file))
        except Exception as e:
            errores.append((file.name, repr(e)))
            print(f"❌ ERROR en {file.name}: {e}")

    if not todos_registros:
        print("No se encontraron registros.")
        return

    df = pd.DataFrame(todos_registros)

    columnas_orden = [
        "archivo", "centro", "tipo_registro", "folio", "empresa", "establecimiento", "ducto",
        "tipo_control", "periodo", "numero_muestra", "tipo_muestra", "fecha_muestreo",
        "hora_inicio_muestreo", "hora_termino_muestreo", "caudal_comprometido", "caudal_informado",
        "laboratorio", "codigo_informe", "parametro", "unidad_medida", "valor_limite",
        "valor_medido", "numero_fila_caudal", "fecha_caudal", "hora_caudal",
    ]
    df = df.reindex(columns=[c for c in columnas_orden if c in df.columns])

    df = limpiar_columna_valor_medido(df)
    df = limpiar_columna_caudal_informado(df)

    cols_sort = [c for c in ["archivo", "numero_muestra", "tipo_registro", "parametro", "fecha_caudal", "hora_caudal", "numero_fila_caudal"] if c in df.columns]
    if cols_sort:
        df = df.sort_values(cols_sort, na_position="last").reset_index(drop=True)

    resumen_mensual = construir_resumen_mensual(df)

    with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="registros", index=False)
        resumen_mensual.to_excel(writer, sheet_name="resumen_mensual", index=False)

    print(f"\n✅ Excel generado en:\n{output_excel.resolve()}")

    if errores:
        print("\n⚠️ PDFs con error:")
        for nombre, err in errores:
            print(f" - {nombre}: {err}")


if __name__ == "__main__":
    main()
