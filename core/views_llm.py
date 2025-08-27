# core/views_llm.py
import json
from asgiref.sync import async_to_sync
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse, HttpResponseBadRequest
from .geo_features import compute_home_work_and_activity
from .models import ClientCity
from services.llm_local import plan_meeting

# core/views_llm.py
def build_context_for_client(client_id: str, period: str = "30d") -> dict:
    feats = compute_home_work_and_activity(
        client_id=str(client_id),
        period=period,
        events=['app_open', 'view', 'Login Success', 'Authorization Success']  # ключевая правка
    )
    places = feats.get("places", [])
    activity = feats.get("activity", {"hourly": [0]*24, "weekday": [0]*7})

    from .models import ClientCity
    city = ClientCity.objects.filter(ac_client_hash=str(client_id)).values_list('city', flat=True).first()

    for p in places:
        if p.get("type") == "home":
            p["label"] = "Дом"
        elif p.get("type") == "work":
            p["label"] = "Работа"
        else:
            p["label"] = p.get("type", "Место")

    return {
        "client_id": str(client_id),
        "city": city or "",
        "places": places,
        "activity": activity,
        "constraints": {
            "meeting_hours_weekday": ["10:00-13:00", "16:00-19:00"],
            "meeting_hours_weekend": ["12:00-17:00"],
        },
    }

@csrf_exempt
@require_POST
def plan_meeting_view(request):
    try:
        body = json.loads(request.body.decode("utf-8"))
        client_id = body.get("client_id")
        period = body.get("period", "30d")
        if not client_id:
            return HttpResponseBadRequest("client_id required")
        ctx = build_context_for_client(client_id, period)
        result = async_to_sync(plan_meeting)(ctx)
        return JsonResponse(result.model_dump(), safe=False)
    except Exception as e:
        return HttpResponseBadRequest(str(e))
