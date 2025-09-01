# core/views_llm.py
import json
import logging
from asgiref.sync import async_to_sync
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse, HttpResponseBadRequest
from django.db.models import Sum, Count

from .geo_features import compute_home_work_and_activity
from .models import ClientCity, Tr
from services.llm_local import plan_meeting

logger = logging.getLogger("llm")

def build_context_for_client(client_id: str, period: str = "30d") -> dict:
    feats = compute_home_work_and_activity(
        client_id=str(client_id),
        period=period,
        events=['Login Success', 'Authorization Success'],
    )
    places = feats.get("places", [])
    activity = feats.get("activity", {"hourly": [0] * 24, "weekday": [0] * 7})

    qs = Tr.objects.filter(
        ac_client_hash=str(client_id),
        t_trx_direction='D',
        c_txn_rub_amt__gt=0,
    )
    top = (
        qs.values('t_merchant_name', 't_trx_city')
          .annotate(amount=Sum('c_txn_rub_amt'), ops=Count('*'))
          .order_by('-amount')[:5]
    )
    merchants = [{
        "name": (row.get('t_merchant_name') or "—"),
        "city": (row.get('t_trx_city') or "—"),
        "amount": float(row.get('amount') or 0),
        "ops": int(row.get('ops') or 0),
    } for row in top]

    city = ClientCity.objects.filter(
        ac_client_hash=str(client_id)
    ).values_list('city', flat=True).first()

    logger.info(
        "LLM: build_ctx places=%s hourly=%s weekday=%s merchants=%s",
        len(places), len(activity.get("hourly", [])), len(activity.get("weekday", [])), len(merchants),
    )

    return {
        "client_id": str(client_id),
        "city": city or "",
        "places": places,
        "activity": activity,
        "merchants_top": merchants,
        "constraints": {
            "meeting_hours_weekday": ["10:00-13:00", "16:00-19:00"],
            "meeting_hours_weekend": ["12:00-17:00"],
        },
    }

@csrf_exempt
@require_POST
def plan_meeting_view(request):
    try:
        logger.info("LLM: request received %s", request.path)

        raw = request.body.decode("utf-8") if request.body else "{}"
        logger.info("LLM: raw body len=%s", len(raw))

        body = json.loads(raw) if raw else {}
        logger.info("LLM: body type=%s keys=%s", type(body).__name__, (list(body.keys())[:5] if isinstance(body, dict) else None))

        if not isinstance(body, dict):
            logger.warning("LLM: invalid body type=%s -> 400", type(body).__name__)
            return HttpResponseBadRequest("Invalid JSON: expected object")

        client_id = body.get("client_id")
        period = body.get("period", "30d")
        logger.info("LLM: params client_id=%s period=%s", str(client_id)[:8], period)

        if not client_id:
            logger.warning("LLM: missing client_id -> 400")
            return HttpResponseBadRequest("client_id required")

        ctx = build_context_for_client(client_id, period)
        logger.info(
            "LLM: ctx summary places=%s merchants=%s",
            len(ctx.get("places", [])), len(ctx.get("merchants_top", [])),
        )

        result = async_to_sync(plan_meeting)(ctx)
        data = result.model_dump()
        logger.info("LLM: model_dump type=%s keys=%s", type(data).__name__, (list(data.keys())[:5] if isinstance(data, dict) else None))

        if not isinstance(data, dict):
            logger.warning("LLM: model_dump returned %s -> wrapping to dict", type(data).__name__)
            data = {
                "appointments": [],
                "habits": [],
                "constraints_used": [],
                "need_clarification": True,
                "questions": ["Некорректный формат ответа модели: ожидался объект."]
            }

        try:
            preview = json.dumps({k: (data[k] if k != "appointments" else f"{len(data[k])} slots") for k in list(data.keys())[:4]}, ensure_ascii=False)
        except Exception:
            preview = str(type(data))
        logger.info("LLM: response preview=%s", preview)

        return JsonResponse(data, safe=False)

    except json.JSONDecodeError as e:
        logger.exception("LLM: JSON decode error: %s", e)
        return HttpResponseBadRequest("Invalid JSON body")
    except Exception as e:
        logger.exception("LLM: unhandled error: %s", e)
        return HttpResponseBadRequest(str(e))
