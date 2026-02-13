"""Основной модуль обработки — оркестрация pipeline.

Поддерживает:
- Параллельное декодирование страниц (multiprocessing)
- Инкрементальную запись в CSV каждые N страниц
- Возобновление после прерывания (resume)
- Корректное завершение по Ctrl+C с сохранением прогресса
"""

import signal
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

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

from .converter import ROI_NORM, get_page_count, render_page
from .decoder import decode_datamatrix
from .exporter import append_to_csv, load_progress
from .models import ProcessingResult, SessionStats, Status
from .parser import normalize_gs1_raw, parse_honest_mark

# Каждые SAVE_EVERY страниц результаты дописываются в CSV
SAVE_EVERY = 50


def _decode_single_page(
    pdf_path_str: str,
    page_num: int,
    dpi: int,
    parse_marks: bool,
) -> list[ProcessingResult]:
    """Обработка одной страницы в отдельном процессе.

    Рендерит страницу → декодирует DataMatrix → парсит ЧЗ.
    Эта функция запускается в ProcessPoolExecutor.

    Args:
        pdf_path_str: Путь к PDF (строка — для pickle-совместимости).
        page_num: Номер страницы (0-based).
        dpi: Разрешение рендеринга.
        parse_marks: Парсить ли как Честный Знак.

    Returns:
        Список ProcessingResult для этой страницы.
    """
    pdf_path = Path(pdf_path_str)
    filename = pdf_path.name
    display_page = page_num + 1  # 1-based для пользователя

    try:
        image = render_page(pdf_path, page_num, dpi=dpi, use_roi=True)
        codes = decode_datamatrix(image)
        del image  # освобождаем RAM

        # Если работаем с ROI и код в зоне не найден — пробуем полный лист.
        if not codes and ROI_NORM is not None:
            full_image = render_page(pdf_path, page_num, dpi=dpi, use_roi=False)
            codes = decode_datamatrix(full_image)
            del full_image

        if codes:
            results = []
            for code_value in codes:
                normalized_code = normalize_gs1_raw(code_value)
                result = ProcessingResult(
                    filename=filename,
                    page=display_page,
                    datamatrix_raw=normalized_code,
                    status=Status.OK,
                )
                if parse_marks:
                    mark = parse_honest_mark(normalized_code)
                    result.gtin = mark.gtin
                    result.serial = mark.serial
                    result.verification_key = mark.verification_key
                    result.crypto = mark.crypto
                results.append(result)
            return results
        else:
            return [
                ProcessingResult(
                    filename=filename,
                    page=display_page,
                    status=Status.NOT_FOUND,
                )
            ]

    except Exception as e:
        return [
            ProcessingResult(
                filename=filename,
                page=display_page,
                status=Status.ERROR,
                error_message=str(e),
            )
        ]


def run(
    input_dir: Path,
    output_path: Path,
    dpi: int = 300,
    parse_marks: bool = True,
    page_limit: int | None = None,
    workers: int = 1,
    resume: bool = False,
) -> SessionStats:
    """Запускает полный pipeline обработки.

    Args:
        input_dir: Директория с PDF-файлами.
        output_path: Путь для CSV-результата.
        dpi: Разрешение рендеринга.
        parse_marks: Парсить ли коды как Честный Знак.
        page_limit: Макс. кол-во страниц (None = все).
        workers: Количество параллельных процессов (1 = последовательно).
        resume: Продолжить с места прерывания.

    Returns:
        SessionStats — общая статистика сессии.
    """
    pdf_files = sorted(input_dir.glob("*.pdf"))
    session = SessionStats(total_files=len(pdf_files))

    if not pdf_files:
        return session

    # --- Resume: загружаем уже обработанные страницы ---
    done_pages: set[tuple[str, int]] = set()
    if resume:
        done_pages = load_progress(output_path)
        session.resumed_from = len(done_pages)

    # --- Строим очередь задач: (pdf_path, page_0based) ---
    task_queue: list[tuple[Path, int]] = []
    for pdf_path in pdf_files:
        try:
            total = get_page_count(pdf_path)
        except Exception as e:
            session.files_with_errors += 1
            session.errors.append(f"{pdf_path.name}: {e}")
            continue

        session.total_pages += total

        for p in range(total):
            display_page = p + 1
            if (pdf_path.name, display_page) in done_pages:
                continue  # пропускаем уже обработанные
            task_queue.append((pdf_path, p))

        session.processed_files += 1

    # Лимит
    if page_limit is not None:
        task_queue = task_queue[:page_limit]
    ordered_page_keys = [(pdf_path.name, page_num + 1) for pdf_path, page_num in task_queue]

    total_to_process = len(task_queue)
    if total_to_process == 0:
        return session

    # --- Буфер и состояние ---
    buffer: list[ProcessingResult] = []
    interrupted = False

    def _flush_buffer() -> None:
        """Сбрасывает буфер в CSV."""
        nonlocal buffer
        if buffer:
            append_to_csv(buffer, output_path)
            buffer = []

    # --- Progress bar ---
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
        page_task = progress.add_task(
            "Страницы", total=total_to_process
        )

        if workers <= 1:
            # === Последовательный режим ===
            try:
                for pdf_path, page_num in task_queue:
                    results = _decode_single_page(
                        str(pdf_path), page_num, dpi, parse_marks
                    )

                    for r in results:
                        buffer.append(r)
                        if r.status == Status.OK:
                            session.total_codes += 1
                        elif r.status == Status.NOT_FOUND:
                            session.pages_empty += 1

                    session.pages_processed += 1
                    progress.advance(page_task)

                    # Инкрементальное сохранение
                    if len(buffer) >= SAVE_EVERY:
                        _flush_buffer()

            except KeyboardInterrupt:
                interrupted = True

        else:
            # === Параллельный режим ===
            # Игнорируем SIGINT в дочерних процессах — корректно завершаем из главного
            original_sigint = signal.getsignal(signal.SIGINT)
            ready_pages: dict[tuple[str, int], list[ProcessingResult]] = {}
            next_page_idx = 0

            def _drain_ready_pages() -> None:
                """Переносит в буфер только последовательные страницы в порядке PDF."""
                nonlocal next_page_idx
                while next_page_idx < len(ordered_page_keys):
                    page_key = ordered_page_keys[next_page_idx]
                    page_results = ready_pages.pop(page_key, None)
                    if page_results is None:
                        break
                    buffer.extend(page_results)
                    next_page_idx += 1
                    if len(buffer) >= SAVE_EVERY:
                        _flush_buffer()

            try:
                with ProcessPoolExecutor(
                    max_workers=workers,
                    initializer=signal.signal,
                    initargs=(signal.SIGINT, signal.SIG_IGN),
                ) as executor:
                    futures = {
                        executor.submit(
                            _decode_single_page,
                            str(pdf_path),
                            page_num,
                            dpi,
                            parse_marks,
                        ): (pdf_path.name, page_num + 1)
                        for pdf_path, page_num in task_queue
                    }

                    try:
                        for future in as_completed(futures):
                            page_key = futures[future]
                            try:
                                results = future.result()
                            except Exception as e:
                                fname, display_page = page_key
                                results = [
                                    ProcessingResult(
                                        filename=fname,
                                        page=display_page,
                                        status=Status.ERROR,
                                        error_message=str(e),
                                    )
                                ]

                            for r in results:
                                if r.status == Status.OK:
                                    session.total_codes += 1
                                elif r.status == Status.NOT_FOUND:
                                    session.pages_empty += 1

                            ready_pages[page_key] = results
                            _drain_ready_pages()

                            session.pages_processed += 1
                            progress.advance(page_task)

                    except KeyboardInterrupt:
                        interrupted = True
                        executor.shutdown(wait=False, cancel_futures=True)

            finally:
                signal.signal(signal.SIGINT, original_sigint)

    # --- Финальный сброс буфера ---
    _flush_buffer()
    session.interrupted = interrupted

    return session
