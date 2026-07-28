"""Lightweight named-entity extraction for ASR/MT text.

Combines regex patterns (numbers, dates, URLs, English runs), jieba POS tagging
for Chinese proper nouns, and an optional team glossary.  Returns character-span
annotations that the frontend renders as highlighted ``<mark>`` tokens.

The result is a list of ``{start, end, text, type}`` dicts with ``start``/``end``
being character offsets into the input string.  Spans are non-overlapping
(greedily resolved: earlier start wins, longer span wins, glossary > regex > jieba).
"""
import re
from typing import Any, Dict, List, Tuple

# jieba POS tags we treat as Chinese proper nouns -> entity type.
_ZH_PROPER_POS = {
    "nr": "person",   # 人名
    "ns": "place",    # 地名
    "nt": "org",      # 机构团体
    "nz": "term",     # 其它专有名词
}

_posseg = None


def _get_posseg():
    """Lazy-load jieba.posseg so the module imports even before jieba is installed."""
    global _posseg
    if _posseg is None:
        import jieba.posseg  # type: ignore
        _posseg = jieba.posseg
    return _posseg


_URL_RE = re.compile(r"https?://[^\s，。、,]+|www\.[^\s，。、,]+")
_DATE_RE = re.compile(
    r"\d{4}[-/年.]\d{1,2}[-/月.]\d{1,2}日?"
    r"|\d{1,2}月\d{1,2}日"
    r"|\d{1,2}[:：]\d{1,2}(?::\d{1,2})?"
)
_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?%?")
_ZH_NUMERAL_RE = re.compile(r"[零一二三四五六七八九十百千万亿两壹贰叁肆伍陆柒捌玖拾佰仟]+")
_ENG_RE = re.compile(r"[A-Za-z]{2,}")
# English proper-noun phrase: "Apple", "New York", "United States".
_EN_PROPER_RE = re.compile(r"[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]+)*")


def _regex_spans(text: str, lang: str) -> List[Tuple[int, int, str, str]]:
    spans: List[Tuple[int, int, str, str]] = []
    for match in _URL_RE.finditer(text):
        spans.append((match.start(), match.end(), match.group(0), "url"))
    for match in _DATE_RE.finditer(text):
        spans.append((match.start(), match.end(), match.group(0), "date"))
    for match in _NUMBER_RE.finditer(text):
        spans.append((match.start(), match.end(), match.group(0), "number"))
    if lang == "zh":
        for match in _ZH_NUMERAL_RE.finditer(text):
            spans.append((match.start(), match.end(), match.group(0), "number"))
        for match in _ENG_RE.finditer(text):
            spans.append((match.start(), match.end(), match.group(0), "eng"))
    else:
        for match in _EN_PROPER_RE.finditer(text):
            spans.append((match.start(), match.end(), match.group(0), "term"))
    return spans


def _jieba_spans(text: str) -> List[Tuple[int, int, str, str]]:
    spans: List[Tuple[int, int, str, str]] = []
    try:
        posseg = _get_posseg()
    except Exception:
        return spans
    offset = 0
    for word, flag in posseg.cut(text):
        start = text.find(word, offset)
        if start == -1:
            offset += len(word)
            continue
        end = start + len(word)
        etype = _ZH_PROPER_POS.get(flag)
        if etype and len(word) >= 2:
            spans.append((start, end, word, etype))
        offset = end
    return spans


def _glossary_spans(text: str, glossary: List[Dict[str, Any]]) -> List[Tuple[int, int, str, str]]:
    spans: List[Tuple[int, int, str, str]] = []
    if not glossary:
        return spans
    lowered = text.lower()
    for entry in glossary:
        if not isinstance(entry, dict):
            continue
        term = str(entry.get("term", "")).strip()
        if len(term) < 2:
            continue
        etype = entry.get("type") or "term"
        needle = term.lower()
        start = 0
        while True:
            idx = lowered.find(needle, start)
            if idx == -1:
                break
            spans.append((idx, idx + len(term), text[idx:idx + len(term)], etype))
            start = idx + len(term)
    return spans


def extract_entities(text: str, lang: str, glossary: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Return non-overlapping entity spans for ``text``.

    ``lang`` is the ASR/MT language of this text (``"zh"`` or ``"en"``); Chinese
    text gets jieba POS tagging in addition to regex.  ``glossary`` is an optional
    list of ``{"term": ..., "type": ...}`` dicts from the team config.
    """
    if not text:
        return []
    lang = (lang or "").lower()
    # (start, end, text, type, priority)  -- higher priority wins ties.
    spans: List[Tuple[int, int, str, str, int]] = []
    spans.extend((s[0], s[1], s[2], s[3], 2) for s in _regex_spans(text, lang))
    if lang == "zh":
        spans.extend((s[0], s[1], s[2], s[3], 1) for s in _jieba_spans(text))
    spans.extend((s[0], s[1], s[2], s[3], 3) for s in _glossary_spans(text, glossary or []))
    if not spans:
        return []
    # Sort by start asc, then length desc, then priority desc.
    spans.sort(key=lambda s: (s[0], -(s[1] - s[0]), -s[4]))
    result: List[Dict[str, Any]] = []
    last_end = -1
    for start, end, seg_text, etype, _pri in spans:
        if start >= last_end:
            result.append({"start": start, "end": end, "text": seg_text, "type": etype})
            last_end = end
    return result
