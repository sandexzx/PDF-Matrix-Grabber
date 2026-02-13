"""Экспорт результатов в CSV — только список DataMatrix-кодов."""

from pathlib import Path
import re

import pandas as pd

from .models import ProcessingResult, Status

# Сохраняем GS (0x1D), остальные управляющие символы экранируем.
_ILLEGAL_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1C\x1E-\x1F]")
_PROGRESS_SUFFIX = ".progress.csv"


def _sanitize_csv_string(value: str) -> str:
    """Экранирует проблемные управляющие символы для CSV (кроме GS)."""

    def _replace(match: re.Match[str]) -> str:
        ch = match.group(0)
        return f"\\x{ord(ch):02x}"

    return _ILLEGAL_CONTROL_CHARS_RE.sub(_replace, value)


def _sanitize_csv_value(value: object) -> object:
    """Подготавливает значение к безопасной записи в CSV."""
    if isinstance(value, str):
        return _sanitize_csv_string(value)
    return value


def _results_to_codes(results: list[ProcessingResult]) -> list[str]:
    """Оставляет только успешные, непустые DataMatrix-коды."""
    codes: list[str] = []
    for result in results:
        if result.status is not Status.OK:
            continue
        if not result.datamatrix_raw:
            continue
        value = _sanitize_csv_value(result.datamatrix_raw)
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
    write_header = not progress_path.exists() or progress_path.stat().st_size == 0
    progress_df = pd.DataFrame(done_pages, columns=["filename", "page"])
    progress_df.to_csv(
        str(progress_path),
        index=False,
        mode="a",
        header=write_header,
    )


def export_to_csv(results: list[ProcessingResult], output_path: Path) -> Path:
    """Экспортирует результаты обработки в CSV-файл.

    Args:
        results: Список результатов обработки.
        output_path: Путь для сохранения файла.

    Returns:
        Path — путь к созданному файлу.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    codes = _results_to_codes(results)
    df = pd.DataFrame(codes)
    df.to_csv(
        str(output_path),
        index=False,
        header=False,
        encoding="utf-8",
        lineterminator="\n",
    )
    _append_progress(results, output_path)
    return output_path


def append_to_csv(results: list[ProcessingResult], output_path: Path) -> None:
    """Дописывает результаты к существующему CSV-файлу.

    Если файл не существует — создаёт новый.
    Если существует — дописывает новые строки в конец.

    Args:
        results: Новые результаты для добавления.
        output_path: Путь к CSV-файлу.
    """
    if not results:
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    _append_progress(results, output_path)

    codes = _results_to_codes(results)
    if not codes:
        return

    new_df = pd.DataFrame(codes)
    new_df.to_csv(
        str(output_path),
        index=False,
        header=False,
        encoding="utf-8",
        lineterminator="\n",
        mode="a",
    )


def load_progress(output_path: Path) -> set[tuple[str, int]]:
    """Загружает уже обработанные страницы из sidecar-файла прогресса.

    Используется для возобновления после прерывания.

    Args:
        output_path: Путь к результирующему файлу.

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

    return set()


# Backward compatibility для существующих импортов.
import warnings as _warnings


def export_to_excel(*args, **kwargs):
    """Deprecated: используйте export_to_csv."""
    _warnings.warn(
        "export_to_excel переименована в export_to_csv и будет удалена в v1.0",
        DeprecationWarning,
        stacklevel=2,
    )
    return export_to_csv(*args, **kwargs)


def append_to_excel(*args, **kwargs):
    """Deprecated: используйте append_to_csv."""
    _warnings.warn(
        "append_to_excel переименована в append_to_csv и будет удалена в v1.0",
        DeprecationWarning,
        stacklevel=2,
    )
    return append_to_csv(*args, **kwargs)
