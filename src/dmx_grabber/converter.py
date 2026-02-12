"""Конвертация PDF-файлов в изображения."""

from pathlib import Path

from pdf2image import convert_from_path
from PIL import Image


def pdf_to_images(
    pdf_path: Path, dpi: int = 300, first_page: int | None = None, last_page: int | None = None
) -> list[Image.Image]:
    """Конвертирует PDF в список PIL-изображений (одна страница = одно изображение).

    Args:
        pdf_path: Путь к PDF-файлу.
        dpi: Разрешение рендеринга. 300 — оптимальный баланс скорости и качества.
        first_page: Номер первой страницы (1-based). None = с начала.
        last_page: Номер последней страницы (1-based). None = до конца.

    Returns:
        Список PIL.Image — по одному на каждую страницу.

    Raises:
        FileNotFoundError: Если файл не найден.
        Exception: При ошибке конвертации (повреждённый PDF и т.д.).
    """
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF не найден: {pdf_path}")

    kwargs: dict = {"pdf_path": str(pdf_path), "dpi": dpi}
    if first_page is not None:
        kwargs["first_page"] = first_page
    if last_page is not None:
        kwargs["last_page"] = last_page

    return convert_from_path(**kwargs)
