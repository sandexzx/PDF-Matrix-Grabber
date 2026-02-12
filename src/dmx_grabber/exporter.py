"""Экспорт результатов в Excel — инкрементальный и финальный."""

from pathlib import Path

import pandas as pd

from .models import ProcessingResult

# Колонки в результирующем файле
COLUMNS = [
    "Файл",
    "Страница",
    "DataMatrix (raw)",
    "GTIN",
    "Серийный номер",
    "Ключ проверки",
    "Криптохвост",
    "Статус",
]


def _result_to_row(r: ProcessingResult) -> dict:
    """Преобразует один результат в строку для DataFrame."""
    return {
        "Файл": r.filename,
        "Страница": r.page,
        "DataMatrix (raw)": r.datamatrix_raw,
        "GTIN": r.gtin,
        "Серийный номер": r.serial,
        "Ключ проверки": r.verification_key,
        "Криптохвост": r.crypto,
        "Статус": r.status.value if hasattr(r.status, "value") else str(r.status),
    }


def export_to_excel(results: list[ProcessingResult], output_path: Path) -> Path:
    """Экспортирует результаты обработки в Excel-файл.

    Args:
        results: Список результатов обработки.
        output_path: Путь для сохранения файла.

    Returns:
        Path — путь к созданному файлу.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [_result_to_row(r) for r in results]
    df = pd.DataFrame(rows, columns=COLUMNS)
    df.to_excel(str(output_path), index=False, engine="openpyxl")
    return output_path


def append_to_excel(results: list[ProcessingResult], output_path: Path) -> None:
    """Дописывает результаты к существующему Excel-файлу.

    Если файл не существует — создаёт новый. Если существует —
    читает текущее содержимое, дописывает новые строки и перезаписывает.

    Args:
        results: Новые результаты для добавления.
        output_path: Путь к Excel-файлу.
    """
    if not results:
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_rows = [_result_to_row(r) for r in results]
    new_df = pd.DataFrame(new_rows, columns=COLUMNS)

    if output_path.exists():
        existing_df = pd.read_excel(str(output_path), engine="openpyxl")
        df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        df = new_df

    df.to_excel(str(output_path), index=False, engine="openpyxl")


def load_progress(output_path: Path) -> set[tuple[str, int]]:
    """Загружает уже обработанные страницы из существующего Excel.

    Используется для возобновления после прерывания.

    Args:
        output_path: Путь к Excel-файлу.

    Returns:
        Множество кортежей (filename, page_num) — уже обработанные.
    """
    if not output_path.exists():
        return set()

    try:
        df = pd.read_excel(str(output_path), engine="openpyxl")
        return {
            (row["Файл"], int(row["Страница"]))
            for _, row in df.iterrows()
            if pd.notna(row.get("Страница"))
        }
    except Exception:
        return set()
