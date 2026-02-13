"""Декодирование DataMatrix кодов из изображений."""

import cv2
import numpy as np
from PIL import Image
from pylibdmtx.pylibdmtx import decode as dmtx_decode

FIRST_PASS_TIMEOUT_MS = 200
SECOND_PASS_TIMEOUT_MS = 800
MAX_CODES_PER_PAGE = 1


def preprocess_image(image: Image.Image) -> Image.Image:
    """Предобработка изображения для улучшения распознавания.

    Конвертирует в оттенки серого, применяет адаптивную бинаризацию
    и увеличивает контраст. Помогает при низком качестве исходников.

    Args:
        image: Исходное PIL-изображение.

    Returns:
        Обработанное PIL-изображение.
    """
    img_array = np.array(image)

    # В оттенки серого
    if len(img_array.shape) == 3:
        gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
    else:
        gray = img_array

    # Адаптивная бинаризация — хорошо работает при неравномерном освещении
    binary = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 51, 15
    )

    return Image.fromarray(binary)


def decode_datamatrix(image: Image.Image, use_preprocessing: bool = True) -> list[str]:
    """Декодирует DataMatrix на изображении.

    Сначала пытается декодировать без предобработки (быстрее).
    Если не нашёл — применяет предобработку и повторяет.
    Оптимизировано под кейс: не более 1 кода на страницу.

    Args:
        image: PIL-изображение страницы.
        use_preprocessing: Использовать ли предобработку при неудаче.

    Returns:
        Список строк — декодированные DataMatrix значения.
    """
    # Первая попытка — без обработки (быстро)
    results = dmtx_decode(
        image,
        timeout=FIRST_PASS_TIMEOUT_MS,
        max_count=MAX_CODES_PER_PAGE,
    )

    if results:
        return [r.data.decode("utf-8", errors="replace") for r in results]

    # Вторая попытка — с предобработкой
    if use_preprocessing:
        processed = preprocess_image(image)
        results = dmtx_decode(
            processed,
            timeout=SECOND_PASS_TIMEOUT_MS,
            max_count=MAX_CODES_PER_PAGE,
        )
        if results:
            return [r.data.decode("utf-8", errors="replace") for r in results]

    return []
