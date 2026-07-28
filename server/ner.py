"""Lightweight named-entity extraction for ASR/MT text.

Combines improved regex patterns (alphanumeric tokens like A100/GPT-4, numbers
with units like 12万/5.2%/12万美元, dates, URLs, English runs), jieba POS tagging
for Chinese proper nouns (boosted by an optional bundled user dictionary under
``server/data/dict/*.txt``), and a team glossary.  Returns character-span
annotations the frontend renders as ``<mark>`` tokens.

The result is a list of ``{start, end, text, type}`` dicts with non-overlapping
spans (greedily resolved: earlier start wins, longer span wins,
glossary > regex > jieba).
"""
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

# jieba POS tags we treat as Chinese proper nouns -> entity type.
_ZH_PROPER_POS = {
    "nr": "person",   # 人名
    "ns": "place",    # 地名
    "nt": "org",      # 机构团体
    "nz": "term",     # 其它专有名词
}

_posseg = None
_userdict_loaded = False


def _load_user_dicts(jieba_module) -> None:
    """Load every ``*.txt`` under server/data/dict/ as a jieba user dictionary.

    Files use the jieba userdict format ``word freq tag`` (freq/tag optional).
    Drop HanLP (Apache-2.0) name/place/org lists or a team term list here and
    they are picked up on first use -- no code change needed.
    """
    dict_dir = Path(__file__).resolve().parent / "data" / "dict"
    if not dict_dir.is_dir():
        return
    for dict_file in sorted(dict_dir.glob("*.txt")):
        try:
            jieba_module.load_userdict(str(dict_file))
        except Exception:
            pass


def _get_posseg():
    """Lazy-load jieba.posseg (and user dicts) so the module imports cleanly
    even before jieba is installed."""
    global _posseg, _userdict_loaded
    if _posseg is None:
        import jieba
        import jieba.posseg  # type: ignore
        _posseg = jieba.posseg
        if not _userdict_loaded:
            _load_user_dicts(jieba)
            _userdict_loaded = True
    return _posseg


_URL_RE = re.compile(r"https?://[^\s，。、,]+|www\.[^\s，。、,]+")
# Dates: 2024-01-15 / 2024年1月15日 / 2024年1月 / 1月15日 / 12:30 / 2024年
_DATE_RE = re.compile(
    r"\d{4}[-/年.]\d{1,2}[-/月.]\d{1,2}日?"
    r"|\d{4}年\d{1,2}月"
    r"|\d{1,2}月\d{1,2}日"
    r"|\d{1,2}[:：]\d{1,2}(?::\d{1,2})?"
    r"|\d{4}年"
)
# Number with optional Chinese multiplier + unit: 12 / 5.2% / 12万 / 12万美元 / 1.7万亿 / 2024年 / 3天
_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?(?:[万亿千百十两]+)?\s*[%％元美元人民币块年月日天时分秒个倍次度吨公斤斤]*")
# Pure Chinese numerals: 一百 / 万亿 / 二零二四 (filtered for length below).
_ZH_NUMERAL_RE = re.compile(r"[零一二三四五六七八九十百千万亿两壹贰叁肆伍陆柒捌玖拾佰仟]+")
# A contiguous alphanumeric (with hyphens) run; we keep those with BOTH a letter
# and a digit as "term": A100, GPT-4, iPhone15, 5G, RTX4090.
_TOKEN_RUN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]*")
# Pure English runs inside Chinese text: GDP, OpenAI, CEO.
_ENG_RE = re.compile(r"[A-Za-z]{2,}")
# English proper-noun phrase in English text: Apple, New York, United States.
_EN_PROPER_RE = re.compile(r"[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]+)*")

# Function words that get capitalised at sentence start but are not entities.
# Filtered out of English proper-noun matches (checked against the first token,
# lower-cased).  Kept to clear function words only -- never includes words that
# can start a real proper noun (e.g. "New", "One").
_EN_STOPWORDS = {
    "the", "this", "that", "these", "those", "there", "their", "them", "they",
    "then", "than", "thus", "though", "although", "but", "and", "or", "nor",
    "so", "if", "as", "at", "by", "for", "in", "of", "on", "to", "up", "out",
    "it", "its", "is", "are", "was", "were", "be", "been", "being", "am",
    "have", "has", "had", "do", "does", "did", "will", "would", "shall",
    "should", "can", "could", "may", "might", "must", "about", "after",
    "before", "between", "into", "from", "with", "without", "when", "where",
    "what", "who", "whom", "whose", "why", "how", "which", "while", "during",
    "he", "she", "we", "us", "our", "you", "your", "said", "says",
}
# General stoplist applied to every candidate span (lower-cased).  Catches the
# few common words jieba/regex might over-tag; kept small to avoid false drops.
_STOPWORDS = _EN_STOPWORDS | {
    "我们", "你们", "他们", "她们", "它们", "大家", "别人", "自己", "什么", "怎么",
    "这样", "那样", "这里", "那里", "哪里", "这个", "那个", "这些", "那些", "时候",
    "地方", "东西", "事情", "问题", "情况", "部分", "方面", "一些", "许多", "其实",
    "而且", "但是", "如果", "虽然", "因为", "所以", "已经", "正在",
}


def _is_noise_number(seg: str) -> bool:
    # Drop bare single-char numbers (e.g. "3", "一", "万") -- too noisy.
    return len(seg) < 2


def _regex_spans(text: str, lang: str) -> List[Tuple[int, int, str, str]]:
    spans: List[Tuple[int, int, str, str]] = []
    for match in _URL_RE.finditer(text):
        spans.append((match.start(), match.end(), match.group(0), "url"))
    for match in _DATE_RE.finditer(text):
        spans.append((match.start(), match.end(), match.group(0), "date"))
    # Alphanumeric tokens (letters + digits) -> term. Pure-letter / pure-digit
    # runs are left to _ENG_RE / _EN_PROPER_RE / _NUMBER_RE below.
    for match in _TOKEN_RUN_RE.finditer(text):
        seg = match.group(0)
        if any(c.isalpha() for c in seg) and any(c.isdigit() for c in seg):
            spans.append((match.start(), match.end(), seg, "term"))
    for match in _NUMBER_RE.finditer(text):
        seg = match.group(0)
        if _is_noise_number(seg):
            continue
        spans.append((match.start(), match.end(), seg, "number"))
    if lang == "zh":
        for match in _ZH_NUMERAL_RE.finditer(text):
            seg = match.group(0)
            if _is_noise_number(seg):
                continue
            spans.append((match.start(), match.end(), seg, "number"))
        for match in _ENG_RE.finditer(text):
            spans.append((match.start(), match.end(), match.group(0), "eng"))
    else:
        spans.extend(_en_proper_spans(text))
    return spans


def _en_proper_spans(text: str) -> List[Tuple[int, int, str, str]]:
    spans: List[Tuple[int, int, str, str]] = []
    for match in _EN_PROPER_RE.finditer(text):
        seg = match.group(0)
        # Skip phrases whose first token is a function word ("The", "This", ...).
        if seg.split()[0].lower() in _EN_STOPWORDS:
            continue
        spans.append((match.start(), match.end(), seg, "term"))
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


# ---- Optional transformers NER backend (opt-in via config ner.use_transformers) ----
# Dedicated per-language NER models -- lighter and more accurate than zero-shot
# GLiNER for standard entity types.  Chinese: CLUENER2020 (10 types).  English:
# CoNLL-03 PER/ORG/LOC/MISC, uncased so it tolerates unstable ASR casing.
_TRANSFORMERS_LABEL_MAP_ZH = {
    "name": "person", "company": "org", "government": "org", "organization": "org",
    "address": "place", "scene": "place", "position": "term",
    "book": "term", "movie": "term", "game": "term",
}
_TRANSFORMERS_LABEL_MAP_EN = {
    "PER": "person", "ORG": "org", "LOC": "place", "MISC": "term",
}
_transformers_pipelines: Dict[str, Any] = {}
_transformers_tried: Dict[str, bool] = {}


def _get_transformers_pipeline(lang: str):
    """Lazy-load the per-language transformers NER pipeline if
    ``ner.use_transformers`` is enabled.  Returns ``None`` when disabled or
    unavailable (caller falls back to jieba).  WARNING: this loads a torch model
    in-process; if the env's numpy/torch stack is binary-incompatible it can
    crash the interpreter -- only enable after verifying the model loads via a
    standalone ``python -c`` first.  Default is off.
    """
    if lang in _transformers_pipelines:
        return _transformers_pipelines[lang]
    if _transformers_tried.get(lang):
        return None
    _transformers_tried[lang] = True
    try:
        from .config import ner_config
        if not ner_config().get("use_transformers"):
            return None
        os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")
        from transformers import pipeline
        cfg = ner_config()
        if lang == "zh":
            model_id = cfg.get("transformers_model_zh") or "uer/roberta-base-finetuned-cluener2020-chinese"
        else:
            model_id = cfg.get("transformers_model_en") or "elastic/distilbert-base-uncased-finetuned-conll03-english"
        _transformers_pipelines[lang] = pipeline("ner", model=model_id, aggregation_strategy="simple")
    except Exception:
        _transformers_pipelines[lang] = None
    return _transformers_pipelines[lang]


def _transformers_spans(text: str, lang: str) -> List[Tuple[int, int, str, str]]:
    spans: List[Tuple[int, int, str, str]] = []
    pipe = _get_transformers_pipeline(lang)
    if pipe is None:
        return spans
    label_map = _TRANSFORMERS_LABEL_MAP_ZH if lang == "zh" else _TRANSFORMERS_LABEL_MAP_EN
    try:
        from .config import ner_config
        threshold = float(ner_config().get("transformers_threshold") or 0.5)
        for ent in pipe(text):
            if float(ent.get("score", 0)) < threshold:
                continue
            start, end = int(ent.get("start", 0)), int(ent.get("end", 0))
            if end <= start:
                continue
            group = str(ent.get("entity_group", ""))
            etype = label_map.get(group) or label_map.get(group.upper()) or label_map.get(group.lower()) or "term"
            spans.append((start, end, text[start:end], etype))
    except Exception:
        pass
    return spans


def extract_entities(text: str, lang: str, glossary: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Return non-overlapping entity spans for ``text``.

    ``lang`` is the language of this text (``"zh"`` or ``"en"``); Chinese text
    gets jieba POS tagging in addition to regex.  ``glossary`` is an optional
    list of ``{"term": ..., "type": ...}`` dicts from the team config.

    If ``ner.use_transformers`` is enabled in config and torch + transformers
    are installed in a compatible env, a dedicated per-language NER model
    (Chinese CLUENER / English CoNLL-03) replaces jieba for proper-noun
    detection; regex (numbers/dates/URLs) and the glossary always apply.
    Opt-in because it needs torch + transformers and a numpy stack this
    project's pinned env may not have.
    """
    if not text:
        return []
    lang = (lang or "").lower()
    # (start, end, text, type, priority) -- higher priority wins ties.
    spans: List[Tuple[int, int, str, str, int]] = []
    spans.extend((s[0], s[1], s[2], s[3], 2) for s in _regex_spans(text, lang))
    model_spans = _transformers_spans(text, lang)
    if model_spans:
        spans.extend((s[0], s[1], s[2], s[3], 2) for s in model_spans)
    elif lang == "zh":
        spans.extend((s[0], s[1], s[2], s[3], 1) for s in _jieba_spans(text))
    spans.extend((s[0], s[1], s[2], s[3], 3) for s in _glossary_spans(text, glossary or []))
    # Drop obvious non-entities (function words etc.) to sharpen precision.
    spans = [s for s in spans if s[2].lower() not in _STOPWORDS]
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
