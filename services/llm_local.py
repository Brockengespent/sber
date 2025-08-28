# services/llm_local.py
import os
import json
from datetime import date, timedelta
from typing import Literal, List, Dict, Any

import httpx
from pydantic import BaseModel, Field, ValidationError, field_validator

# ========= ENV CONFIG =========
LLM_API_URL = os.getenv("LLM_API_URL", "https://api.deepseek.com")
LLM_MODEL = os.getenv("LLM_MODEL", "deepseek-chat")
LLM_TIMEOUT = int(os.getenv("LLM_TIMEOUT", "12"))
LLM_API_KEY = os.getenv("OPENAI_API_KEY") or os.getenv("DEEPSEEK_API_KEY") or ""

# ========= MODELS =========
class Evidence(BaseModel):
    type: str      # "trx" | "login" | "geo"
    when: str      # "вт 12:40–13:10"
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
    date: str       # YYYY-MM-DD
    start: str      # HH:MM
    end: str        # HH:MM
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str
    signals: List[str] = Field(default_factory=list)

    @field_validator("start", "end")
    @classmethod
    def _hhmm(cls, v):
        assert len(v) == 5 and v[13] == ":" and 0 <= int(v[:2]) < 24 and 0 <= int(v[3:]) < 60
        return v


    @field_validator("start", "end")
    @classmethod
    def _hhmm(cls, v):
        assert len(v) == 5 and v[2] == ":" and 0 <= int(v[:2]) < 24 and 0 <= int(v[3:]) < 60
        return v

class PlanResponseV2(BaseModel):
    appointments: List[Appointment] = Field(default_factory=list)
    habits: List[Habit] = Field(default_factory=list)
    constraints_used: List[Dict[str, Any]] = Field(default_factory=list)
    need_clarification: bool = False
    questions: List[str] = Field(default_factory=list)

# ========= PROMPT RULES =========
SYSTEM_RULES_V2 = (
    "Ты планировщик встреч. Верни СТРОГО валидный json-объект без Markdown и без текста вне JSON. "
    "Структура: {\"appointments\":[], \"habits\":[], \"constraints_used\":[], \"need_clarification\": bool, \"questions\": []}. "
    "Используй только переданный контекст: гео-дом/работа, почасовая/по-дневная активность, шаблоны расходов (мерчанты/MCC), регулярные окна. "
    "Дата YYYY-MM-DD, время HH:MM, 1–3 слота. Если данных мало/конфликтуют — need_clarification=true и заполни questions."
)

def _build_messages(context: dict) -> list[dict]:
    return [
        {"role": "system", "content": SYSTEM_RULES_V2},
        {"role": "user", "content": json.dumps(context, ensure_ascii=False)},
    ]

# ========= FALLBACK =========
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
        return Appointment(
            place_type=p["type"],
            label=p.get("label", p["type"].title()),
            lat=float(p["lat"]),
            lon=float(p["lon"]),
            radius_m=int(p.get("radius_m", 300)),
            date=d.isoformat(),
            start=rng,
            end=rng[15],
            confidence=float(p.get("confidence", 0.5)),
            reason=reason,
            signals=["fallback"],
        )


    res = PlanResponseV2(appointments=[], habits=[], constraints_used=[], need_clarification=True, questions=[])
    work = places.get("work")
    home = places.get("home")

    if work and float(work.get("confidence", 0)) >= 0.5:
        for d in next_weekdays(2):
            res.appointments.append(mk(work, d, ("11:00", "13:00"), "Будний дневной слот рядом с работой"))
            if len(res.appointments) >= 2:
                break
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

    res.questions = ["Нет достаточных данных по местам и активности. Уточнить предпочтительное место и дни недели?"]
    return res

# ========= LLM CALL (DeepSeek/OpenAI совместимый) =========
async def _chat_complete(messages: list[dict]) -> str:
    headers = {"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type": "application/json"}
    timeout = httpx.Timeout(connect=3.0, read=float(LLM_TIMEOUT), write=5.0, pool=5.0)

    payload = {
        "model": LLM_MODEL,
        "messages": messages,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},  # стабильный режим для DeepSeek
        "max_tokens": 700,
        "stream": False,
    }

    async with httpx.AsyncClient(base_url=LLM_API_URL, headers=headers, timeout=timeout) as client:
        r = await client.post("/chat/completions", json=payload)
        if r.status_code >= 400:
            try:
                print("LLM ERROR:", r.status_code, r.text[:2000])
            except Exception:
                pass
        r.raise_for_status()
        data = r.json()
        msg = data.get("choices", [{}]).get("message", {}).get("content")
        if not msg:
            raise ValueError(f"LLM response missing content: {data}")
        return msg

# ========= PUBLIC ENTRY =========
async def plan_meeting(context: dict) -> PlanResponseV2:
    messages = _build_messages(context)
    try:
        raw = await _chat_complete(messages)
        data = json.loads(raw)
        return PlanResponseV2(**data)
    except (ValidationError, json.JSONDecodeError, AssertionError, KeyError, ValueError, httpx.HTTPStatusError):
        return _fallback(context)
