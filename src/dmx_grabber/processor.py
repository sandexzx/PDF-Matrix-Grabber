"""Основной модуль обработки — оркестрация всего pipeline."""

from pathlib import Path

from pdf2image import pdfinfo_from_path
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from .converter import pdf_to_images
from .decoder import decode_datamatrix
from .exporter import export_to_excel
from .models import FileStats, ProcessingResult, SessionStats, Status
from .parser import parse_honest_mark

# Размер пакета для конвертации (сколько страниц за раз загружать в память)
BATCH_SIZE = 20


def _process_page(
    image,
    page_num: int,
    filename: str,
    parse_marks: bool,
) -> tuple[list[ProcessingResult], int]:
    """Обработка одной страницы: декодирование + парсинг.

    Returns:
        Кортеж (результаты, количество найденных кодов).
    """
    results: list[ProcessingResult] = []
    codes_found = 0

    codes = decode_datamatrix(image)

    if codes:
        for code_value in codes:
            result = ProcessingResult(
                filename=filename,
                page=page_num,
                datamatrix_raw=code_value,
                status=Status.OK,
            )
            if parse_marks:
                mark = parse_honest_mark(code_value)
                result.gtin = mark.gtin
                result.serial = mark.serial
                result.verification_key = mark.verification_key
                result.crypto = mark.crypto
            results.append(result)
        codes_found = len(codes)
    else:
        results.append(
            ProcessingResult(
                filename=filename,
                page=page_num,
                status=Status.NOT_FOUND,
            )
        )

    return results, codes_found


def process_single_pdf(
    pdf_path: Path,
    dpi: int = 300,
    parse_marks: bool = True,
    page_limit: int | None = None,
    progress: Progress | None = None,
    page_task_id: int | None = None,
    pages_processed_global: int = 0,
) -> tuple[list[ProcessingResult], FileStats, int]:
    """Обрабатывает один PDF-файл: конвертация → декодирование → парсинг.

    Конвертирует страницы пакетами по BATCH_SIZE, чтобы не загружать
    все 3000+ страниц в память разом.

    Args:
        pdf_path: Путь к PDF.
        dpi: Разрешение рендеринга.
        parse_marks: Пытаться ли парсить код как Честный Знак.
        page_limit: Общий лимит страниц для обработки (None = без лимита).
        progress: Rich Progress для обновления прогресса страниц.
        page_task_id: ID задачи прогресса для страниц.
        pages_processed_global: Сколько страниц уже обработано глобально.

    Returns:
        Кортеж (список результатов, статистика файла, обработано страниц глобально).
    """
    filename = pdf_path.name
    stats = FileStats(filename=filename)
    results: list[ProcessingResult] = []

    try:
        # Узнаём количество страниц без конвертации
        info = pdfinfo_from_path(str(pdf_path))
        total_pages = info["Pages"]
        stats.total_pages = total_pages

        # Определяем сколько страниц обработать в этом файле
        if page_limit is not None:
            remaining = page_limit - pages_processed_global
            if remaining <= 0:
                return results, stats, pages_processed_global
            pages_to_process = min(total_pages, remaining)
        else:
            pages_to_process = total_pages

        if progress and page_task_id is not None:
            progress.update(
                page_task_id,
                total=(progress.tasks[page_task_id].total or 0) + pages_to_process,
            )

        # Обрабатываем пакетами
        for batch_start in range(1, pages_to_process + 1, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE - 1, pages_to_process)

            images = pdf_to_images(
                pdf_path, dpi=dpi, first_page=batch_start, last_page=batch_end
            )

            for i, image in enumerate(images):
                page_num = batch_start + i

                page_results, codes_count = _process_page(
                    image, page_num, filename, parse_marks
                )
                results.extend(page_results)
                stats.codes_found += codes_count

                if codes_count == 0:
                    stats.pages_empty += 1

                pages_processed_global += 1

                if progress and page_task_id is not None:
                    progress.advance(page_task_id)

                # Освобождаем память
                del image

            del images

    except Exception as e:
        stats.error = str(e)
        results.append(
            ProcessingResult(
                filename=filename,
                page=None,
                status=f"{Status.ERROR}: {e}",
            )
        )

    return results, stats, pages_processed_global


def run(
    input_dir: Path,
    output_path: Path,
    dpi: int = 300,
    parse_marks: bool = True,
    page_limit: int | None = None,
) -> SessionStats:
    """Запускает полный pipeline обработки.

    Сканирует папку, последовательно обрабатывает каждый PDF,
    собирает статистику, экспортирует результат в Excel.

    Args:
        input_dir: Директория с PDF-файлами.
        output_path: Путь для Excel-результата.
        dpi: Разрешение рендеринга.
        parse_marks: Парсить ли коды как Честный Знак.
        page_limit: Максимальное количество страниц для обработки (None = все).

    Returns:
        SessionStats — общая статистика сессии.
    """
    pdf_files = sorted(input_dir.glob("*.pdf"))
    session = SessionStats(total_files=len(pdf_files))

    if not pdf_files:
        return session

    all_results: list[ProcessingResult] = []
    pages_processed_global = 0

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=40),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        expand=False,
    )

    with progress:
        file_task = progress.add_task("📄 Файлы", total=len(pdf_files))
        page_task = progress.add_task("📃 Страницы", total=0)

        for pdf_path in pdf_files:
            # Проверяем лимит
            if page_limit is not None and pages_processed_global >= page_limit:
                break

            progress.update(file_task, description=f"📄 {pdf_path.name[:40]}")

            results, file_stats, pages_processed_global = process_single_pdf(
                pdf_path,
                dpi=dpi,
                parse_marks=parse_marks,
                page_limit=page_limit,
                progress=progress,
                page_task_id=page_task,
                pages_processed_global=pages_processed_global,
            )

            all_results.extend(results)

            # Обновляем общую статистику
            session.processed_files += 1
            session.total_pages += file_stats.total_pages
            session.pages_processed = pages_processed_global
            session.total_codes += file_stats.codes_found
            session.pages_empty += file_stats.pages_empty

            if file_stats.error:
                session.files_with_errors += 1
                session.errors.append(f"{file_stats.filename}: {file_stats.error}")

            progress.advance(file_task)

    # Экспорт
    if all_results:
        export_to_excel(all_results, output_path)

    return session
