# AGENTS.md

## Project purpose
This repository contains a Python extractor for SINADER PDF certificates that converts them into structured Excel output.

## Core rule
Always use the PDF certificates stored in the repository as the primary validation corpus for extraction changes.

Do not rely only on heuristic assumptions or on one or two manually chosen PDFs.

## What matters most
The priority is to improve extraction quality for:
- Código principal
- Descripción Residuo
- Cantidad (Kg)
- Tratamiento
- Destino
- Transportista
- Patente

while keeping DEFRA behavior stable unless a bug is clearly demonstrated.

## Required workflow for extraction changes
When modifying the SINADER extractor:

1. Inspect the current extraction logic first.
2. Run the extractor against the example PDFs stored in the repo.
3. Identify repeated patterns across the PDFs.
4. Adjust parsing rules using real examples from the repo.
5. Compare output quality before and after changes.
6. Summarize what improved and what still fails.

## Validation corpus
Use the repository PDFs as real test cases, including patterns from:
- Salmofood / Vitapro
- Río Dulce
- Piscicultura Huincacara
- Piscicultura Llaima Cherquen
- monthly declarations
- annual declarations
- single-page and two-page variants
- "Período sin movimientos" cases

## High-priority extraction bugs
Treat these as high priority:
- destination contaminated with residue-description text
- treatment contaminated with destination text
- broken handling of multiline cells
- broken handling of page-to-page table continuation
- treating "1|" as transportista by default
- overwriting good DEFRA values due to dirty treatment parsing

## Do not rules
- Do not use OCR
- Do not rewrite the whole project from scratch
- Do not do a massive refactor unless absolutely necessary
- Do not change DEFRA behavior unless a real bug is demonstrated and documented
- Do not remove "Texto fila original"

## Done when
A change is only done when:
- it was validated using the PDFs in the repo
- output quality measurably improves
- known repeated patterns are parsed more correctly
- no obvious DEFRA regression is introduced

## Output expectations
Prefer minimal, testable changes.
Keep extraction traceability.
Report:
- functions modified
- patterns fixed
- metrics before/after
- remaining fragile cases