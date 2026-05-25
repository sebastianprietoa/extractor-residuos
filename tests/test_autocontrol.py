import unittest
from pathlib import Path
from unittest.mock import patch

from app import autocontrol


OLD_FORMAT_PAGES = [
    "\n".join(
        [
            "Comprobante de Reporte de Autocontrol",
            "SISTEMA DE SEGUIMIENTO DE RESIDUOS LÍQUIDOS",
            "DATOS GENERALES",
            "Folio:",
            "000000098493",
            "Fecha de Ingreso al Sistema:",
            "20/01/2026",
            "Tipo Control:",
            "Autocontrol",
            "Período de Evaluación:",
            "Diciembre 2025",
            "Empresa:",
            "COOKE AQUACULTURE CHILE SA",
            "Establecimiento:",
            "PLANTA EL TEPUAL",
            "Ducto:",
            "PUNTO 1 ESTERO MAÑÍO",
            "Muestra N° 1",
            "Tipo de Muestra:",
            "Compuesta",
            "Fecha de Muestreo:",
            "05/12/2025",
            "Hora Inicio de Muestreo",
            "07:00",
            "Hora Término de Muestreo:",
            "14:30",
            "Caudal Comprometido:",
            "960 m3/dia",
            "Caudal Informado:",
            "254,2 m3/dia",
            "ETFA:",
            "SGS CHILE LTDA SOCIEDAD DE CONTROL - SGS CHILE LTDA SOCIEDAD DE CONTROL",
            "Fecha de Ingreso Laboratorio:",
            "06/12/2025",
            "Código de Informe de Laboratorio:",
            "ES25-80798",
            "Detalle Parámetros Reportados:",
            "Parámetro",
            "Unidad de Medida",
            "Valor Límite",
            "Valor Medido",
            "Aceites y Grasas",
            "mg/L",
            "20",
            "<2",
            "Cloruros",
            "mg/L",
            "400",
            "33",
        ]
    ),
    "\n".join(
        [
            "Caudal",
            "Unidad de Medida: m3/dia",
            "Límite: -",
            "N°",
            "Fecha",
            "Descarga",
            "Valor Medido",
            "1",
            "01/12/2025",
            "618",
            "2",
            "02/12/2025",
            "525,64",
            "pH",
            "Unidad de Medida: Unidad",
            "Límite: 6 - 8,5",
            "N°",
            "Fecha",
            "Descarga",
            "Valor Medido",
            "1",
            "05/12/2025 07:00",
            "6,1",
            "2",
            "05/12/2025 07:30",
            "6,1",
        ]
    ),
    "\n".join(
        [
            "Temperatura",
            "Unidad de Medida: °C",
            "Límite: 35",
            "N°",
            "Fecha",
            "Descarga",
            "Valor Medido",
            "1",
            "05/12/2025 07:00",
            "15,9",
            "2",
            "05/12/2025 07:30",
            "16",
            "Muestra N° 2",
            "Página 3 de 4",
        ]
    ),
]


NEW_FORMAT_PAGES = [
    "\n".join(
        [
            "SISTEMA DE FISCALIZACIÓN DE NORMA EMISIÓN RESIDUOS INDUSTRIALES LÍQUIDOS",
            "Certificado de Autocontrol",
            "Datos Generales",
            "Folio 000000089986 Fecha de Ingreso al Sistema 20-02-2025",
            "Tipo de Control Autocontrol Período de Evaluación 01/2025",
            "RUT 96.926.970-8 Fecha Envío 20-02-2025",
            "Empresa COOKE AQUACULTURE CHILE SA",
            "Establecimiento PLANTA EL TEPUAL",
            "Ducto PUNTO 1 ESTERO MAÑÍO",
        ]
    ),
    "\n".join(
        [
            "SISTEMA DE FISCALIZACIÓN DE NORMA EMISIÓN RESIDUOS INDUSTRIALES LÍQUIDOS",
            "Muestra 1",
            "Código de Informe de Laboratorio ES2501508 Tipo de Muestra Puntual",
            "Nombre Laboratorio SGS Chile Ltda. / Laboratorio Ambiental Sector Environmental Services- Santiago",
            "Material/Producto Proceso Plan de Muestreo Tabla 1 DS 90",
            "Fecha de Ingreso 08-01-2025 Fecha de Muestreo 07-01-2025",
            "Hora Inicio de Muestreo 08:00 Hora Termino de Muestreo 08:00",
            "Lugar de Muestreo Cámara de muestreo",
            "Caudal Comprometido 960 Unidad Medida m3/dia",
            "Caudal 285,9",
            "Parámetros Unidad de Medida Valor Límite Valor Medido",
            "Caudal m3/dia 960 160,2",
            "Coliformes Fecales o Termotolerantes NMP/100 ml 1000 <2",
            "pH Unidad 6 - 8,5 6,6",
            "Temperatura °C 35 16,7",
        ]
    ),
]


class AutocontrolParserTests(unittest.TestCase):
    def test_parsear_pdf_old_format_mixto(self):
        with patch.object(autocontrol, "extraer_textos_paginas", return_value=OLD_FORMAT_PAGES):
            registros = autocontrol.parsear_pdf(Path("comprobante_old.pdf"))

        self.assertEqual(len(registros), 8)
        self.assertTrue(any(r["parametro"] == "Aceites y Grasas" for r in registros))
        self.assertTrue(any(r["parametro"] == "Cloruros" for r in registros))
        self.assertTrue(
            any(
                r["parametro"] == "Caudal diario"
                and r["numero_fila_caudal"] == "1"
                and r["fecha_caudal"] == "01/12/2025"
                and r["valor_medido"] == "618"
                for r in registros
            )
        )
        self.assertTrue(
            any(
                r["parametro"] == "pH"
                and r["numero_fila_caudal"] == "1"
                and r["fecha_caudal"] == "05/12/2025"
                and r["hora_caudal"] == "07:00"
                and r["valor_medido"] == "6,1"
                for r in registros
            )
        )
        self.assertTrue(
            any(
                r["parametro"] == "Temperatura"
                and r["numero_fila_caudal"] == "2"
                and r["valor_medido"] == "16"
                for r in registros
            )
        )

    def test_parsear_pdf_new_format_puntual_generico(self):
        with patch.object(autocontrol, "extraer_textos_paginas", return_value=NEW_FORMAT_PAGES):
            registros = autocontrol.parsear_pdf(Path("certificado_nuevo.pdf"))

        self.assertEqual(len(registros), 4)
        self.assertTrue(any(r["parametro"] == "Caudal" for r in registros))
        self.assertTrue(any(r["parametro"] == "Coliformes Fecales o Termotolerantes" for r in registros))
        self.assertTrue(any(r["parametro"] == "pH" for r in registros))
        self.assertTrue(any(r["parametro"] == "Temperatura" for r in registros))
        self.assertTrue(any(r["tipo_muestra"] == "Puntual" for r in registros))
        self.assertTrue(any(r["codigo_informe"] == "ES2501508" for r in registros))
        self.assertTrue(any(r["caudal_informado"] == "285,9" for r in registros))


if __name__ == "__main__":
    unittest.main()
