"""Конвертация PDF-файлов в изображения через PyMuPDF (fitz).

PyMuPDF в 3-5 раз быстрее pdf2image/poppler при рендеринге страниц,
т.к. работает через MuPDF напрямую, без промежуточного вызова pdftoppm.
"""

from pathlib import Path

import fitz  # PyMuPDF
from PIL import Image


def get_page_count(pdf_path: Path) -> int:
    """Возвращает количество страниц в PDF без полной загрузки."""
    with fitz.open(str(pdf_path)) as doc:
        return doc.page_count


def render_page(pdf_path: Path, page_num: int, dpi: int = 300) -> Image.Image:
    """Рендерит одну страницу PDF в PIL-изображение.

    Args:
        pdf_path: Путь к PDF.
        page_num: Номер страницы (0-based).
        dpi: Разрешение рендеринга.

    Returns:
        PIL.Image одной страницы.
    """
    zoom = dpi / 72  # fitz работает в 72 dpi по умолчанию
    matrix = fitz.Matrix(zoom, zoom)

    with fitz.open(str(pdf_path)) as doc:
        page = doc.load_page(page_num)
        pix = page.get_pixmap(matrix=matrix, alpha=False)
        return Image.frombytes("RGB", (pix.width, pix.height), pix.samples)


def render_pages_batch(
    pdf_path: Path,
    start: int,
    end: int,
    dpi: int = 300,
) -> list[tuple[int, Image.Image]]:
    """Рендерит пакет страниц за одно открытие файла.

    Args:
        pdf_path: Путь к PDF.
        start: Начальная страница (0-based, inclusive).
        end: Конечная страница (0-based, exclusive).
        dpi: Разрешение.

    Returns:
        Список кортежей (page_num_0based, PIL.Image).
    """
    zoom = dpi / 72
    matrix = fitz.Matrix(zoom, zoom)
    results = []

    with fitz.open(str(pdf_path)) as doc:
        for page_num in range(start, min(end, doc.page_count)):
            page = doc.load_page(page_num)
            pix = page.get_pixmap(matrix=matrix, alpha=False)
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            results.append((page_num, img))

    return results
