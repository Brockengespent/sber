# services/llm_local.py
import os, re, json
import logging
from datetime import date, timedelta
from typing import Literal, List, Dict, Any
import httpx
from pydantic import BaseModel, Field, ValidationError, field_validator

logger = logging.getLogger("llm")

# ====== ENV ======
LLM_API_URL = os.getenv("LLM_API_URL", "https://api.deepseek.com")
LLM_MODEL = os.getenv("LLM_MODEL", "deepseek-chat")
LLM_TIMEOUT = int(os.getenv("LLM_TIMEOUT", "12"))
LLM_API_KEY = os.getenv("OPENAI_API_KEY") or os.getenv("DEEPSEEK_API_KEY") or ""

# ====== TIME NORMALIZATION ======
_TIME_PATTERNS = [
    re.compile(r"^\s*(\d{1,2}):(\d{1,2})\s*$"),
    re.compile(r"^\s*(\d{1,2})[.\-](\d{1,2})\s*$"),
    re.compile(r"^\s*(\d{2})(\d{2})\s*$"),
]

def _normalize_hhmm(value: str) -> str:
    if value is None:
        raise ValueError("empty time")
    v = str(value).strip()
    if v == "" or v == "—":
        raise ValueError("empty time")
    for pat in _TIME_PATTERNS:
        m = pat.match(v)
        if m:
            hh, mm = int(m.group(1)), int(m.group(2))
            if 0 <= hh < 24 and 0 <= mm < 60:
                return f"{hh:02d}:{mm:02d}"
    nums = re.findall(r"\d{1,2}", v)
    if len(nums) >= 2:
        hh, mm = int(nums), int(nums[4])
        if 0 <= hh < 24 and 0 <= mm < 60:
            return f"{hh:02d}:{mm:02d}"
    raise ValueError(f"invalid time format: {v}")

# ====== SCHEMA ======
class Evidence(BaseModel):
    type: str
    when: str
    details: str

class Habit(BaseModel):
    pattern: str
    evidence: List[Evidence] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)

class Appointment(BaseModel):
    place_type: Literal["home", "work", "merchant", "neutral"]
    label: str
    lat: float
    lon: float
    radius_m: int = Field(ge=50, le=5000)
    date: str  # YYYY-MM-DD
    start: str # HH:MM
    end: str   # HH:MM
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str
    signals: List[str] = Field(default_factory=list)

    @field_validator("date")
    @classmethod
    def _iso_date(cls, v):
        from datetime import date as _d
        _d.fromisoformat(v)
        return v

    @field_validator("start", "end", mode="before")
    @classmethod
    def _hhmm(cls, v):
        return _normalize_hhmm(v)

class PlanResponseV2(BaseModel):
    appointments: List[Appointment] = Field(default_factory=list)
    habits: List[Habit] = Field(default_factory=list)
    constraints_used: List[Dict[str, Any]] = Field(default_factory=list)
    need_clarification: bool = False
    questions: List[str] = Field(default_factory=list)

# ====== PROMPT ======
SYSTEM_RULES_V2 = (
    "Ты планировщик встреч. Верни строго json-объект БЕЗ Markdown и без текста вне JSON. "
    "Структура: {\"appointments\":[], \"habits\":[], \"constraints_used\":[], \"need_clarification\": bool, \"questions\": []}. "
    "Дата строго YYYY-MM-DD, время строго HH:MM. Разрешённые place_type: home|work|merchant|neutral. Дай 1–3 слота. "
    "Если данных мало — need_clarification=true и вопросы в questions. "
    "Всегда используй русский язык во всех строковых полях (label, reason, questions)."
)


def _build_messages(context: dict) -> list[dict]:
    return [
        {"role": "system", "content": SYSTEM_RULES_V2},
        {"role": "user", "content": json.dumps(context, ensure_ascii=False)},
    ]

# ====== PRE-CLEAN ======
def _coerce_plan(data: dict) -> dict:
    cleaned = []
    for s in (data.get("appointments") or []):
        try:
            if "start" in s: s["start"] = _normalize_hhmm(s["start"])
            if "end" in s:   s["end"]   = _normalize_hhmm(s["end"])
            cleaned.append(s)
        except Exception:
            continue
    # Нормализуем constraints_used к списку словарей
    cu = data.get("constraints_used") or []
    norm = []
    for c in cu:
        if isinstance(c, dict):
            norm.append(c)
        elif isinstance(c, str):
            norm.append({"id": c})
    data["appointments"] = cleaned
    data["constraints_used"] = norm
    return data

# ====== FALLBACK ======
def _fallback(context: dict) -> PlanResponseV2:
    places = {p.get("type"): p for p in context.get("places", [])}
    today = date.today()

    def next_weekdays(n=3):
        out, d = [], today
        while len(out) < n:
            d += timedelta(days=1)
            if d.weekday() < 5:
                out.append(d)
        return out

    def mk(p, d, rng, reason) -> Appointment:
        start_s, end_s = rng
        return Appointment(
            place_type=p["type"],
            label=p.get("label", p["type"].title()),
            lat=float(p["lat"]), lon=float(p["lon"]),
            radius_m=int(p.get("radius_m", 300)),
            date=d.isoformat(),
            start=_normalize_hhmm(start_s), end=_normalize_hhmm(end_s),
            confidence=float(p.get("confidence", 0.5)),
            reason=reason, signals=["fallback"],
        )

    res = PlanResponseV2()
    work = places.get("work"); home = places.get("home")

    if work and float(work.get("confidence", 0)) >= 0.5:
        for d in next_weekdays(2):
            res.appointments.append(mk(work, d, ("11:00", "13:00"), "Будний дневной слот рядом с работой"))
            if len(res.appointments) >= 2: break
        res.need_clarification = False
        return res

    if home and float(home.get("confidence", 0)) >= 0.5:
        d = today
        for _ in range(10):
            d += timedelta(days=1)
            if d.weekday() >= 5:
                res.appointments.append(mk(home, d, ("12:00", "16:00"), "Выходной дневной слот рядом с домом"))
                res.need_clarification = False
                break
        return res

    res.need_clarification = True
    res.questions = ["Недостаточно данных о местах. Уточнить удобные районы и часы?"]
    return res

# ====== LLM CALL ======
async def _chat_complete(messages: list[dict]) -> str:
    headers = {"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type": "application/json"}
    timeout = httpx.Timeout(connect=3.0, read=float(LLM_TIMEOUT), write=5.0, pool=5.0)
    payload = {
        "model": LLM_MODEL,
        "messages": messages,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},  # JSON mode
        "max_tokens": 700,
        "stream": False,
    }

    async with httpx.AsyncClient(base_url=LLM_API_URL, headers=headers, timeout=timeout) as client:
        r = await client.post("/chat/completions", json=payload)
        if r.status_code >= 400:
            r.raise_for_status()

        data = r.json()
        logger.info("LLM: http=%s model=%s choices_len=%s", r.status_code, data.get("model"), len(data.get("choices") or []))

        # Безопасный извлекатель контента из разных форматов
        def _extract_content_safe(obj, max_depth: int = 6) -> str | None:
            seen = set()
            def _key(o):
                try: return id(o)
                except Exception: return None
            def _walk(o, depth: int) -> str | None:
                if depth < 0: return None
                kid = _key(o)
                if kid is not None:
                    if kid in seen: return None
                    seen.add(kid)

                if isinstance(o, dict):
                    msg = o.get("message")
                    if isinstance(msg, dict) and isinstance(msg.get("content"), str):
                        return msg["content"]
                    if isinstance(msg, list) and msg:
                        first = msg
                        if isinstance(first, dict):
                            if isinstance(first.get("content"), str): return first["content"]
                            if isinstance(first.get("text"), str): return first["text"]
                    if isinstance(o.get("content"), str): return o["content"]
                    if isinstance(o.get("text"), str): return o["text"]
                    msgs = o.get("messages")
                    if isinstance(msgs, list) and msgs:
                        got = _walk(msgs, depth - 1)
                        if got: return got
                    delta = o.get("delta")
                    if isinstance(delta, dict) and isinstance(delta.get("content"), str):
                        return delta["content"]

                if isinstance(o, list) and o:
                    for i in range(min(len(o), 3)):
                        got = _walk(o[i], depth - 1)
                        if got: return got
                return None
            return _walk(obj, max_depth)

        choices = data.get("choices") or []
        msg = None
        if isinstance(choices, list) and choices:
            msg = _extract_content_safe(choices)

        if not msg and isinstance(data, dict):
            for k in ("content", "text"):
                if isinstance(data.get(k), str):
                    msg = data[k]; break

        logger.info("LLM: content head=%s", (msg or "")[:120].replace("\n"," "))
        if not msg:
            try:
                logger.info("LLM RAW: %s", json.dumps(data, ensure_ascii=False)[:500])
            except Exception:
                pass
            raise ValueError("LLM response missing content")
        return msg

# ====== PUBLIC ======
async def plan_meeting(context: dict) -> PlanResponseV2:
    logger.info("LLM: call start places=%s merchants=%s", len(context.get("places", [])), len(context.get("merchants_top", [])))
    messages = _build_messages(context)
    try:
        raw = await _chat_complete(messages)
        logger.info("LLM: raw len=%s head=%s", len(raw or ""), (raw or "")[:200].replace("\n"," "))
        data = json.loads(raw)
        logger.info("LLM: parsed type=%s keys=%s", type(data).__name__, (list(data.keys())[:5] if isinstance(data, dict) else None))
        data = _coerce_plan(data)
        logger.info("LLM: after coerce appointments=%s", len(data.get("appointments", [])) if isinstance(data, dict) else None)
        return PlanResponseV2(**data)
    except (ValidationError, json.JSONDecodeError, AssertionError, KeyError, ValueError, httpx.HTTPStatusError) as e:
        logger.exception("LLM: fallback due to error: %s", e)
        return _fallback(context)
