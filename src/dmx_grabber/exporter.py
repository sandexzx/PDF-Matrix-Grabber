"""Экспорт результатов в Excel — только список DataMatrix-кодов."""

from pathlib import Path
import re

import pandas as pd

from .models import ProcessingResult

_ILLEGAL_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
_PROGRESS_SUFFIX = ".progress.csv"


def _sanitize_excel_string(value: str) -> str:
    """Удаляет/экранирует запрещённые для Excel управляющие символы."""

    def _replace(match: re.Match[str]) -> str:
        ch = match.group(0)
        if ch == "\x1d":
            return "<GS>"
        return f"\\x{ord(ch):02x}"

    return _ILLEGAL_CONTROL_CHARS_RE.sub(_replace, value)


def _sanitize_excel_value(value: object) -> object:
    """Подготавливает значение к безопасной записи в Excel."""
    if isinstance(value, str):
        return _sanitize_excel_string(value)
    return value


def _is_ok_status(status: object) -> bool:
    """Определяет, что запись успешно декодирована."""
    if status == "OK":
        return True
    if hasattr(status, "value") and getattr(status, "value") == "OK":
        return True
    return str(status) in {"OK", "Status.OK"}


def _results_to_codes(results: list[ProcessingResult]) -> list[str]:
    """Оставляет только успешные, непустые DataMatrix-коды."""
    codes: list[str] = []
    for result in results:
        if not _is_ok_status(result.status):
            continue
        if not result.datamatrix_raw:
            continue
        value = _sanitize_excel_value(result.datamatrix_raw)
        if isinstance(value, str):
            codes.append(value)
    return codes


def _results_to_done_pages(results: list[ProcessingResult]) -> list[tuple[str, int]]:
    """Собирает обработанные страницы для режима resume."""
    done_pages: list[tuple[str, int]] = []
    for result in results:
        if result.page is None:
            continue
        done_pages.append((result.filename, int(result.page)))
    return done_pages


def _get_progress_path(output_path: Path) -> Path:
    """Возвращает путь к служебному файлу прогресса."""
    return output_path.with_suffix(f"{output_path.suffix}{_PROGRESS_SUFFIX}")


def _append_progress(results: list[ProcessingResult], output_path: Path) -> None:
    """Дописывает обработанные страницы в sidecar-файл для resume."""
    done_pages = _results_to_done_pages(results)
    if not done_pages:
        return

    progress_path = _get_progress_path(output_path)
    progress_df = pd.DataFrame(done_pages, columns=["filename", "page"])
    progress_df.to_csv(
        str(progress_path),
        index=False,
        mode="a",
        header=not progress_path.exists(),
    )


def _read_existing_codes(output_path: Path) -> pd.DataFrame:
    """Читает существующий Excel и нормализует до одного столбца кодов."""
    try:
        df = pd.read_excel(
            str(output_path),
            engine="openpyxl",
            header=None,
            dtype="string",
        )
        if df.shape[1] <= 1:
            return df
    except Exception:
        pass

    # fallback для старого формата с заголовками и многими столбцами
    try:
        old_df = pd.read_excel(str(output_path), engine="openpyxl", dtype="string")
        if "DataMatrix (raw)" in old_df.columns:
            return old_df[["DataMatrix (raw)"]]
        if old_df.shape[1] > 0:
            return old_df.iloc[:, :1]
    except Exception:
        return pd.DataFrame()

    return pd.DataFrame()


def export_to_excel(results: list[ProcessingResult], output_path: Path) -> Path:
    """Экспортирует результаты обработки в Excel-файл.

    Args:
        results: Список результатов обработки.
        output_path: Путь для сохранения файла.

    Returns:
        Path — путь к созданному файлу.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    codes = _results_to_codes(results)
    df = pd.DataFrame(codes)
    df.to_excel(str(output_path), index=False, header=False, engine="openpyxl")
    _append_progress(results, output_path)
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
    _append_progress(results, output_path)

    codes = _results_to_codes(results)
    if not codes:
        return

    new_df = pd.DataFrame(codes)

    if output_path.exists():
        existing_df = _read_existing_codes(output_path)
        df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        df = new_df

    df.to_excel(str(output_path), index=False, header=False, engine="openpyxl")


def load_progress(output_path: Path) -> set[tuple[str, int]]:
    """Загружает уже обработанные страницы из существующего Excel.

    Используется для возобновления после прерывания.

    Args:
        output_path: Путь к Excel-файлу.

    Returns:
        Множество кортежей (filename, page_num) — уже обработанные.
    """
    progress_path = _get_progress_path(output_path)
    if progress_path.exists():
        try:
            progress_df = pd.read_csv(str(progress_path))
            return {
                (str(row["filename"]), int(row["page"]))
                for _, row in progress_df.iterrows()
            }
        except Exception:
            return set()

    if not output_path.exists():
        return set()

    try:
        df = pd.read_excel(str(output_path), engine="openpyxl")
        if "Файл" not in df.columns or "Страница" not in df.columns:
            return set()
        return {
            (row["Файл"], int(row["Страница"]))
            for _, row in df.iterrows()
            if pd.notna(row.get("Страница"))
        }
    except Exception:
        return set()
