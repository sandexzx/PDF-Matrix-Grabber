"""Парсинг кодов маркировки Честный Знак (GS1 DataMatrix)."""

from dataclasses import dataclass
import re

# GS (Group Separator) — разделитель полей в GS1
GS = "\x1d"
AI_SERIAL = "21"
AI_KEYS = ("91", "93")
AI_CRYPTO = "92"
_VISIBLE_GS_RE = re.compile(
    r"(?i)(<GS>|\[GS\]|\{GS\}|␝|\\x1d|\\u001d|\^\])"
)
_LEADING_PREFIXES = ("]d2", "<FNC1>")
_AI_AFTER_GS = ("91", "92", "93")


@dataclass
class HonestMarkCode:
    """Структура кода маркировки Честный Знак.

    Базовый формат: 01 + GTIN(14) + 21 + Serial(до 20) + [GS] + (91|93) + Key(4) + [92 + Crypto]
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


def normalize_gs1_raw(raw_code: str) -> str:
    """Нормализует код GS1 к единому виду с разделителем GS (0x1D)."""
    code = raw_code.strip()

    # У разных сканеров возможны префиксы symbology/FNC1.
    changed = True
    while changed and code:
        changed = False
        for prefix in _LEADING_PREFIXES:
            if code.startswith(prefix):
                code = code[len(prefix) :]
                changed = True
        # В ряде интеграций FNC1 приходит как байт 0xE8.
        if code and ord(code[0]) == 0xE8:
            code = code[1:]
            changed = True
        if code.startswith(GS):
            code = code[1:]
            changed = True

    # Нормализуем видимые/экранированные формы GS в реальный 0x1D.
    code = _VISIBLE_GS_RE.sub(GS, code)

    # Эвристика для интеграций, где разделитель ошибочно передан буквами "GS".
    # Подменяем только маркеры перед служебными AI 91/92/93 и только в основной части кода.
    for ai in _AI_AFTER_GS:
        marker = f"GS{ai}"
        start = 18  # 01 + GTIN(14) + 21 + минимум 1 символ serial
        while True:
            idx = code.find(marker, start)
            if idx == -1:
                break
            code = f"{code[:idx]}{GS}{code[idx + 2:]}"
            start = idx + 1

    return code


def parse_honest_mark(raw_code: str) -> HonestMarkCode:
    """Парсит строку DataMatrix в структуру Честного Знака.

    Поддерживает коды с GS-разделителями и без них.
    Не бросает исключений — при ошибке возвращает объект с raw и None-полями.

    Args:
        raw_code: Сырая строка из DataMatrix.

    Returns:
        HonestMarkCode с распарсенными полями.
    """
    code = normalize_gs1_raw(raw_code)
    result = HonestMarkCode(raw=code)

    try:
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

        # AI 21 — Serial (переменная длина, до GS или до следующего AI)
        if code_rest.startswith(AI_SERIAL):
            serial_data = code_rest[2:]

            # Ищем конец серийника — GS-символ или служебные AI.
            gs_pos = serial_data.find(GS)
            ai_candidates = [serial_data.find(ai) for ai in (*AI_KEYS, AI_CRYPTO)]
            ai_positions = [pos for pos in ai_candidates if pos != -1 and pos <= 20]
            # Некоторые интеграции подставляют буквальный "GS" вместо 0x1D.
            plain_gs_candidates = [
                serial_data.find(f"GS{ai}") for ai in (*AI_KEYS, AI_CRYPTO)
            ]
            plain_gs_positions = [
                pos for pos in plain_gs_candidates if pos != -1 and pos <= 20
            ]

            if gs_pos != -1:
                result.serial = serial_data[:gs_pos]
                code_rest = serial_data[gs_pos + 1 :]
            elif plain_gs_positions:
                next_ai_pos = min(plain_gs_positions)
                result.serial = serial_data[:next_ai_pos]
                code_rest = serial_data[next_ai_pos + 2 :]
            elif ai_positions:
                next_ai_pos = min(ai_positions)
                result.serial = serial_data[:next_ai_pos]
                code_rest = serial_data[next_ai_pos:]
            else:
                result.serial = serial_data[:20]
                code_rest = serial_data[20:]

        # AI 91/93 — ключ проверки (4 символа)
        key_ai = None
        key_pos = -1
        for ai in AI_KEYS:
            pos = code_rest.find(ai)
            if pos != -1 and (key_pos == -1 or pos < key_pos):
                key_ai = ai
                key_pos = pos

        if key_ai is not None and key_pos != -1:
            key = code_rest[key_pos + 2 : key_pos + 6]
            if len(key) == 4:
                result.verification_key = key
            code_rest = code_rest[key_pos + 6 :]

        if code_rest.startswith("GS"):
            code_rest = code_rest[2:]
        if code_rest.startswith(GS):
            code_rest = code_rest[1:]

        # AI 92 — Криптохвост (остаток)
        if AI_CRYPTO in code_rest:
            idx = code_rest.index(AI_CRYPTO)
            result.crypto = code_rest[idx + 2 :]

    except (ValueError, IndexError):
        pass

    return result
