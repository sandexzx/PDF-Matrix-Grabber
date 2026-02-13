"""Модели данных для результатов обработки."""

from dataclasses import dataclass, field
from enum import Enum


class Status(str, Enum):
    """Статус обработки страницы."""

    OK = "OK"
    NOT_FOUND = "НЕ НАЙДЕН"
    ERROR = "ОШИБКА"


@dataclass
class ProcessingResult:
    """Результат обработки одной страницы PDF."""

    filename: str
    page: int | None
    datamatrix_raw: str | None = None
    gtin: str | None = None
    serial: str | None = None
    verification_key: str | None = None
    crypto: str | None = None
    status: Status = Status.OK
    error_message: str | None = None


@dataclass
class SessionStats:
    """Общая статистика сессии обработки."""

    total_files: int = 0
    processed_files: int = 0
    total_pages: int = 0
    pages_processed: int = 0
    total_codes: int = 0
    pages_empty: int = 0
    files_with_errors: int = 0
    errors: list[str] = field(default_factory=list)
    resumed_from: int = 0        # сколько страниц пропущено при resume
    interrupted: bool = False    # было ли прервано по Ctrl+C

    @property
    def success_rate(self) -> float:
        """Процент страниц с найденными кодами."""
        if self.pages_processed == 0:
            return 0.0
        return (self.pages_processed - self.pages_empty) / self.pages_processed * 100
