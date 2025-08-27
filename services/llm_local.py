# services/llm_local.py
import json
import httpx
from datetime import date
from pydantic import BaseModel, Field, ValidationError, field_validator

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "qwen2.5:14b"  # твоя локальная модель

class Appointment(BaseModel):
    place_type: str  # "home"|"work"|"frequent"
    label: str
    lat: float
    lon: float
    radius_m: int = Field(ge=50, le=5000)
    date: str       # YYYY-MM-DD
    start: str      # HH:MM
    end: str        # HH:MM
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str

    @field_validator("date")
    @classmethod
    def _iso_date(cls, v):
        date.fromisoformat(v)
        return v

    @field_validator("start", "end")
    @classmethod
    def _hhmm(cls, v):
        assert len(v) == 5 and v[2] == ":" and 0 <= int(v[:2]) < 24 and 0 <= int(v[3:]) < 60
        return v

class PlanResponse(BaseModel):
    appointments: list[Appointment] = Field(default_factory=list)
    alternatives: list[Appointment] = Field(default_factory=list)
    need_clarification: bool = False

SYSTEM_RULES = (
    "Ты планировщик встреч. Верни СТРОГО валидный JSON, без текста вне JSON.\n"
    'Схема: {"appointments":[...], "alternatives":[...], "need_clarification": boolean}.\n'
    "Используй только переданный контекст. Дата YYYY-MM-DD, время HH:MM. 1–3 слота.\n"
    "Если данных мало — need_clarification=true и пустые appointments.\n"
)

def _build_prompt(context: dict) -> str:
    return SYSTEM_RULES + "Контекст в JSON:\n" + json.dumps(context, ensure_ascii=False)

async def _ollama_call(prompt: str) -> str:
    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "temperature": 0.2,
        "format": "json",   # просим строго JSON
        "options": {"num_ctx": 8192},
        "stream": False
    }
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(OLLAMA_URL, json=payload)
        r.raise_for_status()
        data = r.json()
        return data.get("response", "")

def _fallback(context: dict) -> PlanResponse:
    # простая эвристика на случай невалидного ответа
    from datetime import timedelta
    places = {p["type"]: p for p in context.get("places", [])}
    today = date.today()

    def next_weekdays(n=3):
        out, d = [], today
        while len(out) < n:
            d += timedelta(days=1)
            if d.weekday() < 5:
                out.append(d)
        return out

    def mk(p, d, rng, reason):
        return Appointment(
            place_type=p["type"], label=p.get("label", p["type"].title()),
            lat=p["lat"], lon=p["lon"], radius_m=int(p.get("radius_m", 300)),
            date=d.isoformat(), start=rng[0], end=rng[1],
            confidence=float(p.get("confidence", 0.5)), reason=reason
        )

    res = PlanResponse(appointments=[], alternatives=[], need_clarification=True)
    work = places.get("work")
    home = places.get("home")

    if work and float(work.get("confidence", 0)) >= 0.5:
        for d in next_weekdays(2):
            res.appointments.append(mk(work, d, ("11:00","13:00"), "Будний дневной слот рядом с работой"))
            if len(res.appointments) >= 2:
                break
        res.need_clarification = False
        return res

    if home and float(home.get("confidence", 0)) >= 0.5:
        # ближайший выходной
        d = today
        for _ in range(10):
            d += timedelta(days=1)
            if d.weekday() >= 5:
                res.appointments.append(mk(home, d, ("12:00","16:00"), "Выходной дневной слот рядом с домом"))
                res.need_clarification = False
                break
        return res

    return res  # нет уверенных мест — просим уточнение

async def plan_meeting(context: dict) -> PlanResponse:
    prompt = _build_prompt(context)
    try:
        raw = await _ollama_call(prompt)
        data = json.loads(raw)
        return PlanResponse(**data)
    except (ValidationError, json.JSONDecodeError, AssertionError):
        # один ретрай строже
        strict = prompt + "\nВНИМАНИЕ: верни только JSON по схеме, без комментариев."
        try:
            raw = await _ollama_call(strict)
            data = json.loads(raw)
            return PlanResponse(**data)
        except Exception:
            return _fallback(context)
