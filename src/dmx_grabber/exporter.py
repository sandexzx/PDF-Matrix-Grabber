"""Экспорт результатов в Excel."""

from pathlib import Path

import pandas as pd

from .models import ProcessingResult


def export_to_excel(results: list[ProcessingResult], output_path: Path) -> Path:
    """Экспортирует результаты обработки в Excel-файл.

    Создаёт файл с колонками: файл, страница, DataMatrix, GTIN, серийный номер,
    ключ проверки, криптохвост, статус.

    Args:
        results: Список результатов обработки.
        output_path: Путь для сохранения файла.

    Returns:
        Path — путь к созданному файлу.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for r in results:
        row = {
            "Файл": r.filename,
            "Страница": r.page,
            "DataMatrix (raw)": r.datamatrix_raw,
            "GTIN": r.gtin,
            "Серийный номер": r.serial,
            "Ключ проверки": r.verification_key,
            "Криптохвост": r.crypto,
            "Статус": r.status.value if hasattr(r.status, 'value') else str(r.status),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_excel(str(output_path), index=False, engine="openpyxl")

    return output_path
