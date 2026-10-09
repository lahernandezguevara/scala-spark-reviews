#!/usr/bin/env python3
"""Auditoría del CSV Rest-Mex *antes* de ReviewReader (trim, Option, MD5, -1).

Uso:
  python3 scripts/raw_dataset_audit.py
  python3 scripts/raw_dataset_audit.py /ruta/Rest-Mex_2025_train.csv
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path

EXPECTED_COLUMNS = ["Title", "Review", "Polarity", "Town", "Region", "Type"]
VALID_TYPES = {"Hotel", "Restaurant", "Attractive"}
VALID_POLARITY = {1, 2, 3, 4, 5}
PLACEHOLDERS = {"na", "n/a", "null", "none", "nan", "-", ".", "?"}
MAX_EXAMPLES = 5

DEFAULT_PATH = Path(
    "/mnt/c/Users/elcal/Downloads/Rest-Mex_2025_Train_DataSet/Rest-Mex_2025_train.csv"
)


def is_blank(value: str | None) -> bool:
    return value is None or value.strip() == ""


def is_placeholder(value: str | None) -> bool:
    return value is not None and value.strip().lower() in PLACEHOLDERS


def parse_polarity(raw: str | None) -> str:
    """Clasifica el string crudo; aún no aplica las reglas de ReviewReader."""
    if is_blank(raw):
        return "ausente"
    text = raw.strip()
    try:
        number = float(text)
    except ValueError:
        return "no_numerica"
    if not math.isfinite(number):
        return "no_numerica"
    if number != math.floor(number):
        return "decimal_no_entero"
    as_int = int(number)
    if as_int not in VALID_POLARITY:
        return "fuera_de_rango"
    if "." in text or "e" in text.lower():
        return "entera_en_formato_float"  # p.ej. 5.0 → ReviewReader sí la acepta
    return "entera_valida"


def take_example(bucket: list, item) -> None:
    if len(bucket) < MAX_EXAMPLES:
        bucket.append(item)


def audit(path: Path) -> None:
    if not path.is_file():
        sys.exit(f"No existe el archivo: {path}")

    with path.open("rb") as binary:
        first = binary.readline()
    has_bom = first.startswith(b"\xef\xbb\xbf")

    with path.open(newline="", encoding="utf-8-sig", errors="replace") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            sys.exit("CSV vacío")

    header_counts = Counter(header)
    duplicate_columns = sorted(name for name, n in header_counts.items() if n > 1)
    missing_expected = [c for c in EXPECTED_COLUMNS if c not in header_counts]
    unexpected = [c for c in header if c not in EXPECTED_COLUMNS]

    print("=" * 64)
    print("AUDITORÍA RAW (sin transformaciones de ReviewReader)")
    print("=" * 64)
    print(f"Archivo: {path}")
    print(f"BOM UTF-8: {has_bom}")
    print(f"Columnas en header ({len(header)}): {header}")
    print(f"Nombres de columna duplicados: {duplicate_columns or 'ninguno'}")
    print(f"Columnas esperadas ausentes: {missing_expected or 'ninguna'}")
    print(f"Columnas extra: {unexpected or 'ninguna'}")
    print()

    if missing_expected:
        sys.exit("Faltan columnas obligatorias; se detiene el barrido de filas.")

    # DictReader colapsa nombres duplicados: se detectaron arriba; el barrido
    # usa la última ocurrencia de cada nombre, igual que un lector ingenuo.
    blank = Counter()
    placeholder = Counter()
    polarity_kind = Counter()
    type_values = Counter()
    exact_rows: dict[tuple, list[int]] = defaultdict(list)
    title_review: dict[tuple[str, str], list[int]] = defaultdict(list)

    examples = defaultdict(list)
    n = 0

    with path.open(newline="", encoding="utf-8-sig", errors="replace") as handle:
        reader = csv.DictReader(handle)
        for row_index, row in enumerate(reader):
            n += 1
            title = row.get("Title")
            review = row.get("Review")
            polarity = row.get("Polarity")
            town = row.get("Town")
            region = row.get("Region")
            place_type = row.get("Type")

            for col, value in (
                ("Title", title),
                ("Review", review),
                ("Polarity", polarity),
                ("Town", town),
                ("Region", region),
                ("Type", place_type),
            ):
                if is_blank(value):
                    blank[col] += 1
                elif is_placeholder(value):
                    placeholder[col] += 1
                    take_example(
                        examples[f"placeholder_{col}"],
                        (row_index, repr(value)),
                    )

            if is_blank(title):
                take_example(
                    examples["title_vacio"],
                    (row_index, (review or "")[:80].replace("\n", " ")),
                )
            elif is_placeholder(title):
                take_example(
                    examples["title_invalido"],
                    (row_index, repr(title), (review or "")[:60].replace("\n", " ")),
                )

            kind = parse_polarity(polarity)
            polarity_kind[kind] += 1
            if kind not in {"entera_valida", "entera_en_formato_float"}:
                take_example(examples["polarity_invalida"], (row_index, repr(polarity), kind))

            type_key = (place_type or "").strip()
            if is_blank(place_type):
                type_values["<ausente>"] += 1
            elif type_key not in VALID_TYPES:
                type_values[f"<invalido:{place_type!r}>"] += 1
                take_example(examples["type_invalido"], (row_index, repr(place_type)))
            else:
                type_values[type_key] += 1

            exact_key = tuple(row.get(col) for col in EXPECTED_COLUMNS)
            exact_rows[exact_key].append(row_index)
            title_review[(title or "", review or "")].append(row_index)

    print(f"Filas parseadas (csv.DictReader, multilínea respetada): {n}")
    print()
    print("--- Ausentes / en blanco (None o solo espacios; ANTES de trim→Option) ---")
    for col in EXPECTED_COLUMNS:
        print(f"  {col:10s} {blank[col]:7d}")

    print()
    print("--- Placeholders (Na, null, -, etc.; ReviewReader los dejaría como texto) ---")
    if not placeholder:
        print("  ninguno")
    else:
        for col in EXPECTED_COLUMNS:
            if placeholder[col]:
                print(f"  {col:10s} {placeholder[col]:7d}")

    print()
    print("--- Polaridad cruda (string del CSV, sin toDouble/toInt) ---")
    for kind in (
        "entera_valida",
        "entera_en_formato_float",
        "ausente",
        "no_numerica",
        "decimal_no_entero",
        "fuera_de_rango",
    ):
        print(f"  {kind:28s} {polarity_kind[kind]:7d}")
    invalid_polarity = (
        polarity_kind["ausente"]
        + polarity_kind["no_numerica"]
        + polarity_kind["decimal_no_entero"]
        + polarity_kind["fuera_de_rango"]
    )
    print(f"  {'invalidas_para_target':28s} {invalid_polarity:7d}")
    print("  nota: '5.0' cuenta como entera_en_formato_float; Quality/Reader la aceptan como 5.")

    print()
    print("--- Type ---")
    for name, count in sorted(type_values.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {name:24s} {count:7d}")

    exact_dup_groups = {k: idxs for k, idxs in exact_rows.items() if len(idxs) > 1}
    extra_exact = sum(len(idxs) - 1 for idxs in exact_dup_groups.values())
    tr_dup_groups = {k: idxs for k, idxs in title_review.items() if len(idxs) > 1}
    extra_tr = sum(len(idxs) - 1 for idxs in tr_dup_groups.values())

    print()
    print("--- Duplicados de filas (contenido crudo, no IDs) ---")
    print(f"  grupos fila completa idéntica: {len(exact_dup_groups)}")
    print(f"  filas extra por copia exacta:  {extra_exact}")
    print(f"  grupos Title+Review idénticos: {len(tr_dup_groups)}")
    print(f"  filas extra Title+Review:      {extra_tr}")

    print()
    print("--- Ejemplos (máx. {MAX_EXAMPLES} por categoría) ---".format(MAX_EXAMPLES=MAX_EXAMPLES))
    if examples["title_vacio"]:
        print("  Title vacío/blanco:")
        for row_index, preview in examples["title_vacio"]:
            print(f"    fila {row_index}: review={preview!r}")
    if examples["title_invalido"]:
        print("  Title placeholder:")
        for item in examples["title_invalido"]:
            print(f"    fila {item[0]}: title={item[1]} review={item[2]!r}")
    if examples["polarity_invalida"]:
        print("  Polaridad inválida:")
        for row_index, raw, kind in examples["polarity_invalida"]:
            print(f"    fila {row_index}: {raw} ({kind})")
    else:
        print("  Polaridad inválida: ninguna")
    if examples["type_invalido"]:
        print("  Type inválido:")
        for row_index, raw in examples["type_invalido"]:
            print(f"    fila {row_index}: {raw}")
    shown = 0
    print("  Copias exactas (primeros grupos):")
    for key, idxs in exact_dup_groups.items():
        title_preview = (key[0] or "")[:50]
        print(f"    índices {idxs} title={title_preview!r} town={key[3]!r}")
        shown += 1
        if shown >= MAX_EXAMPLES:
            break
    if not exact_dup_groups:
        print("    ninguna")
    print("=" * 64)


def main() -> None:
    parser = argparse.ArgumentParser(description="Auditoría raw del CSV Rest-Mex")
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=str(DEFAULT_PATH),
        help="Ruta al CSV original",
    )
    args = parser.parse_args()
    audit(Path(args.csv_path))


if __name__ == "__main__":
    main()
