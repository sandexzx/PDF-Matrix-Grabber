"""Парсинг кодов маркировки Честный Знак (GS1 DataMatrix)."""

from dataclasses import dataclass

# GS (Group Separator) — разделитель полей в GS1
GS = "\x1d"


@dataclass
class HonestMarkCode:
    """Структура кода маркировки Честный Знак.

    Формат: 01 + GTIN(14) + 21 + Serial(до 20) + GS + 91 + Key(4) + 92 + Crypto(до 88)
    """

    raw: str
    gtin: str | None = None
    serial: str | None = None
    verification_key: str | None = None
    crypto: str | None = None

    @property
    def is_valid(self) -> bool:
        """Проверяет, что основные поля заполнены."""
        return bool(self.gtin and self.serial)


def parse_honest_mark(raw_code: str) -> HonestMarkCode:
    """Парсит строку DataMatrix в структуру Честного Знака.

    Поддерживает коды с GS-разделителями и без них.
    Не бросает исключений — при ошибке возвращает объект с raw и None-полями.

    Args:
        raw_code: Сырая строка из DataMatrix.

    Returns:
        HonestMarkCode с распарсенными полями.
    """
    result = HonestMarkCode(raw=raw_code)

    try:
        code = raw_code

        # AI 01 — GTIN (всегда 14 цифр)
        if "01" in code:
            idx = code.index("01")
            gtin = code[idx + 2 : idx + 16]
            if len(gtin) == 14 and gtin.isdigit():
                result.gtin = gtin
                code_rest = code[idx + 16 :]
            else:
                return result
        else:
            return result

        # AI 21 — Serial (переменная длина, до GS или до AI 91)
        if code_rest.startswith("21"):
            serial_data = code_rest[2:]

            # Ищем конец серийника — GS-символ или AI 91
            gs_pos = serial_data.find(GS)
            ai91_pos = serial_data.find("91")

            if gs_pos != -1:
                result.serial = serial_data[:gs_pos]
                code_rest = serial_data[gs_pos + 1 :]
            elif ai91_pos != -1 and ai91_pos <= 20:
                result.serial = serial_data[:ai91_pos]
                code_rest = serial_data[ai91_pos:]
            else:
                result.serial = serial_data[:20]
                code_rest = serial_data[20:]

        # AI 91 — Ключ проверки (4 символа)
        if "91" in code_rest:
            idx = code_rest.index("91")
            key = code_rest[idx + 2 : idx + 6]
            if len(key) == 4:
                result.verification_key = key
            code_rest = code_rest[idx + 6 :]

        # AI 92 — Криптохвост (остаток)
        if "92" in code_rest:
            idx = code_rest.index("92")
            result.crypto = code_rest[idx + 2 :]

    except (ValueError, IndexError):
        pass

    return result
