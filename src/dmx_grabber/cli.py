"""CLI интерфейс с Rich-оформлением."""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

from . import __version__
from .models import SessionStats
from .processor import run


console = Console()


BANNER = r"""
 ___  __  ____  _  _     ___  ____   __   ____  ____  ____  ____
(   \(  )(  __)( \/ )   / __)(  _ \ / _\ (  _ \(  _ \(  __)(  _ \
 ) D ()(  ) _)  )  (   ( (_ \ )   //    \ ) _ ( ) _ ( ) _)  )   /
(____/(__)(____)(_/\_)   \___/(__\_)\_/\_/(____/(____/(____)(__\_)
"""


def build_stats_table(stats: SessionStats, elapsed: float) -> Table:
    """Строит Rich-таблицу со статистикой сессии."""
    table = Table(
        title="📊 Статистика обработки",
        box=box.ROUNDED,
        show_header=False,
        title_style="bold cyan",
        border_style="cyan",
        padding=(0, 2),
    )
    table.add_column("Метрика", style="bold white", min_width=25)
    table.add_column("Значение", style="bold green", justify="right", min_width=15)

    table.add_row("📁 Всего файлов", str(stats.total_files))
    table.add_row("✅ Обработано файлов", str(stats.processed_files))
    table.add_row("📃 Всего страниц", str(stats.total_pages))
    table.add_row("� Обработано страниц", str(stats.pages_processed))
    table.add_row("🔍 Найдено кодов", str(stats.total_codes))
    table.add_row("⚠️  Страниц без кодов", str(stats.pages_empty))
    table.add_row("❌ Файлов с ошибками", str(stats.files_with_errors))
    table.add_row("📈 Успешность", f"{stats.success_rate:.1f}%")
    table.add_row("⏱️  Время работы", format_elapsed(elapsed))

    if stats.pages_processed > 0:
        speed = elapsed / stats.pages_processed
        table.add_row("⚡ Скорость", f"{speed:.2f} сек/стр.")

    return table


def build_errors_table(errors: list[str]) -> Table:
    """Строит таблицу с ошибками."""
    table = Table(
        title="❌ Ошибки",
        box=box.ROUNDED,
        title_style="bold red",
        border_style="red",
    )
    table.add_column("#", style="dim", width=4)
    table.add_column("Описание", style="red")

    for i, err in enumerate(errors, 1):
        table.add_row(str(i), err)

    return table


def format_elapsed(seconds: float) -> str:
    """Форматирует секунды в человеко-читаемый вид."""
    if seconds < 60:
        return f"{seconds:.1f} сек"
    minutes = int(seconds // 60)
    secs = seconds % 60
    if minutes < 60:
        return f"{minutes} мин {secs:.0f} сек"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours} ч {mins} мин"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Парсит аргументы командной строки."""
    parser = argparse.ArgumentParser(
        prog="dmx-grabber",
        description="Массовое считывание DataMatrix кодов из PDF файлов",
    )
    parser.add_argument(
        "-i", "--input",
        type=Path,
        default=Path("data/input"),
        help="Директория с PDF файлами (по умолчанию: data/input)",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=None,
        help="Путь для Excel-файла с результатами (по умолчанию: output/results_<timestamp>.xlsx)",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=300,
        help="Разрешение рендеринга PDF (по умолчанию: 300)",
    )
    parser.add_argument(
        "--no-parse",
        action="store_true",
        help="Не парсить коды как Честный Знак",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Максимальное количество страниц для обработки (для тестирования)",
    )
    parser.add_argument(
        "-v", "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Главная точка входа."""

    args = parse_args(argv)

    # Баннер
    console.print(Text(BANNER, style="bold cyan"))
    console.print(
        Panel(
            f"[bold]DMX Grabber[/bold] v{__version__}\n"
            f"Массовое считывание DataMatrix кодов из PDF",
            border_style="cyan",
            padding=(0, 2),
        )
    )

    # Проверка входной директории
    input_dir = args.input.resolve()
    if not input_dir.exists():
        console.print(f"\n[bold red]✖ Директория не найдена:[/] {input_dir}")
        console.print("[dim]Создайте папку и поместите в неё PDF файлы.[/dim]")
        return 1

    pdf_count = len(list(input_dir.glob("*.pdf")))
    if pdf_count == 0:
        console.print(f"\n[bold yellow]⚠ PDF файлы не найдены в:[/] {input_dir}")
        return 1

    # Формируем путь для результата
    if args.output:
        output_path = args.output.resolve()
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path("output").resolve() / f"results_{timestamp}.xlsx"

    # Инфо перед запуском
    console.print()
    info_table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    info_table.add_column("", style="bold")
    info_table.add_column("")
    info_table.add_row("📂 Входная папка:", str(input_dir))
    info_table.add_row("📊 Найдено PDF:", str(pdf_count))
    info_table.add_row("💾 Результат:", str(output_path))
    info_table.add_row("🔧 DPI:", str(args.dpi))
    info_table.add_row("🏷️  Парсинг ЧЗ:", "Да" if not args.no_parse else "Нет")
    if args.limit:
        info_table.add_row("🔢 Лимит страниц:", str(args.limit))
    console.print(info_table)
    console.print()

    # Запуск обработки
    start_time = time.monotonic()

    try:
        stats = run(
            input_dir=input_dir,
            output_path=output_path,
            dpi=args.dpi,
            parse_marks=not args.no_parse,
            page_limit=args.limit,
        )
    except KeyboardInterrupt:
        console.print("\n[bold yellow]⚠ Обработка прервана пользователем.[/]")
        return 130
    except Exception as e:
        console.print(f"\n[bold red]✖ Критическая ошибка:[/] {e}")
        return 1

    elapsed = time.monotonic() - start_time

    # Вывод статистики
    console.print()
    console.print(build_stats_table(stats, elapsed))

    if stats.errors:
        console.print()
        console.print(build_errors_table(stats.errors))

    # Итог
    console.print()
    if stats.total_codes > 0:
        console.print(
            Panel(
                f"[bold green]✔ Готово![/] Результат сохранён:\n[link={output_path}]{output_path}[/link]",
                border_style="green",
            )
        )
    else:
        console.print(
            Panel(
                "[bold yellow]⚠ Коды не были найдены ни на одной странице.[/]\n"
                "Попробуйте увеличить DPI (--dpi 600) или проверьте PDF файлы.",
                border_style="yellow",
            )
        )

    return 0
