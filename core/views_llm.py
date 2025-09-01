# core/views_llm.py
import json
from asgiref.sync import async_to_sync
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse, HttpResponseBadRequest
from django.db.models import Sum, Count, Value
from django.db.models.functions import Coalesce

from .geo_features import compute_home_work_and_activity
from .models import ClientCity, Tr
from services.llm_local import plan_meeting

def build_context_for_client(client_id: str, period: str = "30d") -> dict:
    feats = compute_home_work_and_activity(
        client_id=str(client_id),
        period=period,
        events=['Login Success', 'Authorization Success']  # только входы
    )
    places = feats.get("places", [])
    activity = feats.get("activity", {"hourly":[]*24, "weekday":[]*7})

    # Топ мерчантов по расходам (D)
    qs = Tr.objects.filter(ac_client_hash=str(client_id), t_trx_direction='D', c_txn_rub_amt__gt=0)
    top = (qs.values('t_merchant_name','t_trx_city')
             .annotate(amount=Sum('c_txn_rub_amt'), ops=Count('*'))
             .order_by('-amount')[:5])
    merchants = [{
        "name": (row['t_merchant_name'] or "—"),
        "city": (row['t_trx_city'] or "—"),
        "amount": float(row['amount'] or 0),
        "ops": int(row['ops'] or 0),
    } for row in top]

    city = ClientCity.objects.filter(ac_client_hash=str(client_id)).values_list('city', flat=True).first()

    return {
        "client_id": str(client_id),
        "city": city or "",
        "places": places,            # содержит home/work гипотезы
        "activity": activity,        # почасовая/по дням
        "merchants_top": merchants,  # подсказки «Пятерочка/Магнит/Dodo»
        "constraints": {
            "meeting_hours_weekday": ["10:00-13:00","16:00-19:00"],
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
