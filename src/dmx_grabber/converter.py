"""Конвертация PDF-файлов в изображения через PyMuPDF (fitz).

PyMuPDF в 3-5 раз быстрее pdf2image/poppler при рендеринге страниц,
т.к. работает через MuPDF напрямую, без промежуточного вызова pdftoppm.
"""

from pathlib import Path

import fitz  # PyMuPDF
from PIL import Image

# Нормализованная область (x0, y0, x1, y1) в долях страницы.
# Пример: (0.20, 0.39, 0.81, 0.75)
# None = рендерить всю страницу.
ROI_NORM: tuple[float, float, float, float] | None = None


def _build_clip_rect(page_rect: fitz.Rect) -> fitz.Rect | None:
    """Возвращает clip-прямоугольник в координатах страницы."""
    if ROI_NORM is None:
        return None

    x0, y0, x1, y1 = ROI_NORM
    if not (0 <= x0 < x1 <= 1 and 0 <= y0 < y1 <= 1):
        raise ValueError(
            "ROI_NORM must be normalized to [0..1] and satisfy x0<x1, y0<y1"
        )

    return fitz.Rect(
        page_rect.x0 + page_rect.width * x0,
        page_rect.y0 + page_rect.height * y0,
        page_rect.x0 + page_rect.width * x1,
        page_rect.y0 + page_rect.height * y1,
    )


def get_page_count(pdf_path: Path) -> int:
    """Возвращает количество страниц в PDF без полной загрузки."""
    with fitz.open(str(pdf_path)) as doc:
        return doc.page_count


def render_page(
    pdf_path: Path,
    page_num: int,
    dpi: int = 300,
    use_roi: bool = True,
) -> Image.Image:
    """Рендерит одну страницу PDF в PIL-изображение.

    Args:
        pdf_path: Путь к PDF.
        page_num: Номер страницы (0-based).
        dpi: Разрешение рендеринга.
        use_roi: Применять ли ROI_NORM (если задана).

    Returns:
        PIL.Image одной страницы.
    """
    zoom = dpi / 72  # fitz работает в 72 dpi по умолчанию
    matrix = fitz.Matrix(zoom, zoom)

    with fitz.open(str(pdf_path)) as doc:
        page = doc.load_page(page_num)
        clip_rect = _build_clip_rect(page.rect) if use_roi else None
        pix = page.get_pixmap(matrix=matrix, alpha=False, clip=clip_rect)
        return Image.frombytes("RGB", (pix.width, pix.height), pix.samples)


def render_pages_batch(
    pdf_path: Path,
    start: int,
    end: int,
    dpi: int = 300,
    use_roi: bool = True,
) -> list[tuple[int, Image.Image]]:
    """Рендерит пакет страниц за одно открытие файла.

    Args:
        pdf_path: Путь к PDF.
        start: Начальная страница (0-based, inclusive).
        end: Конечная страница (0-based, exclusive).
        dpi: Разрешение.
        use_roi: Применять ли ROI_NORM (если задана).

    Returns:
        Список кортежей (page_num_0based, PIL.Image).
    """
    zoom = dpi / 72
    matrix = fitz.Matrix(zoom, zoom)
    results = []

    with fitz.open(str(pdf_path)) as doc:
        for page_num in range(start, min(end, doc.page_count)):
            page = doc.load_page(page_num)
            clip_rect = _build_clip_rect(page.rect) if use_roi else None
            pix = page.get_pixmap(matrix=matrix, alpha=False, clip=clip_rect)
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            results.append((page_num, img))

    return results
