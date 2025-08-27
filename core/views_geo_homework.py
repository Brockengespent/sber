# core/views_geo_homework.py
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from django.utils import timezone
from django.utils.timezone import make_aware, is_naive
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from core.models import Cs


def _parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if is_naive(dt):
            dt = make_aware(dt)
        return dt
    except Exception:
        return None


def _apply_period(dt_from: Optional[datetime], dt_to: Optional[datetime], period: Optional[str]):
    if period in ('7d', '30d', '90d'):
        days = int(period[:-1])
        end = timezone.now()
        start = end - timedelta(days=days)
        return start, end
    if period == 'all':
        return None, None
    return dt_from, dt_to


def _in_night(dt: datetime) -> bool:
    # 22:00–06:00 локального времени
    loc = dt.astimezone(timezone.get_current_timezone())
    return loc.hour >= 22 or loc.hour < 6


def _in_workday(dt: datetime) -> bool:
    # Пн–Пт и 09:00–18:00 локального времени
    loc = dt.astimezone(timezone.get_current_timezone())
    return loc.weekday() < 5 and 9 <= loc.hour < 18


def _round_coord(v: float, digits: int = 4) -> float:
    return round(float(v), digits)


def _most_frequent_cell(rows: List[Dict[str, Any]], digits: int = 4):
    """
    rows: [{'lat':..., 'lon':..., 'dt':...}, ...]
    Возвращает словарь с lat, lon, confidence(доля), size(кол-во в ячейке), share(доля от всех), last_seen.
    """
    if not rows:
        return None
    cells: Dict[tuple, int] = {}
    last_seen: Dict[tuple, datetime] = {}
    for r in rows:
        key = (_round_coord(r['lat'], digits), _round_coord(r['lon'], digits))
        cells[key] = cells.get(key, 0) + 1
        if key not in last_seen or r['dt'] > last_seen[key]:
            last_seen[key] = r['dt']

    key_max = max(cells, key=lambda k: cells[k])
    size_max = cells[key_max]
    total = len(rows)
    share = size_max / total if total else 0.0
    return {
        'lat': key_max[0],
        'lon': key_max[1],
        'confidence': share,     # доля в своём классе (ночь/день)
        'size': size_max,
        'share': share,
        'last_seen': last_seen.get(key_max),
    }


def _build_activity(rows: List[Dict[str, Any]]):
    """
    Возвращает почасовую и по дням недели активность по dt (локальное время).
    """
    hourly = [0] * 24
    weekday = [0] * 7
    for r in rows:
        loc = r['dt'].astimezone(timezone.get_current_timezone())
        hourly[loc.hour] += 1
        weekday[loc.weekday()] += 1
    return hourly, weekday


class HomeWorkAPI(APIView):
    """
    GET /api/geo/homework/?client_id=...&period=7d|30d|90d|all&datetime_from=...&datetime_to=...&events=...
    Ответ:
      {
        "home": {lat, lon, confidence, size, share, last_seen} | null,
        "work": {lat, lon, confidence, size, share, last_seen} | null,
        "features": {
          "hourly_activity": [24 ints],
          "weekday_activity": [7 ints],
          "counts": {"total": int, "night": int, "work": int}
        }
      }
    """

    def get(self, request):
        client_id = request.GET.get('client_id')
        if not client_id:
            return Response({"detail": "client_id is required"}, status=status.HTTP_400_BAD_REQUEST)

        period = request.GET.get('period')  # '7d'|'30d'|'90d'|'all'
        dt_from = _parse_iso_dt(request.GET.get('datetime_from'))
        dt_to = _parse_iso_dt(request.GET.get('datetime_to'))
        dt_from, dt_to = _apply_period(dt_from, dt_to, period)

        events: List[str] = ['Login Success', 'Authorization Success']

        try:
            cid = int(client_id)
        except Exception:
            cid = client_id

        qs = Cs.objects.filter(
            ac_client_hash=cid,
            eventaction__in=events
        ).exclude(geolatitude__isnull=True).exclude(geolongitude__isnull=True) \
         .exclude(geolatitude=0).exclude(geolongitude=0) \
         .filter(geolatitude__gte=-85.0, geolatitude__lte=85.0,
                 geolongitude__gte=-180.0, geolongitude__lte=180.0)

        if dt_from and dt_to:
            qs = qs.filter(dt__gte=dt_from, dt__lte=dt_to)
        elif dt_from and not dt_to:
            qs = qs.filter(dt__gte=dt_from)
        elif dt_to and not dt_from:
            qs = qs.filter(dt__lte=dt_to)

        rows = list(qs.order_by('-dt').values('dt', 'geolatitude', 'geolongitude'))

        # Разделение на ночь/рабочее время и подготовка кластера
        night, workd = [], []
        for r in rows:
            dt = r['dt']
            lat = float(r['geolatitude']); lon = float(r['geolongitude'])
            item = {'dt': dt, 'lat': lat, 'lon': lon}
            if _in_night(dt):
                night.append(item)
            if _in_workday(dt):
                workd.append(item)

        home = _most_frequent_cell(night, digits=4)
        work = _most_frequent_cell(workd, digits=4)

        hourly, weekday = _build_activity(rows)

        data = {
            'home': home,
            'work': work,
            'features': {
                'hourly_activity': hourly,
                'weekday_activity': weekday,
                'counts': {
                    'total': len(rows),
                    'night': len(night),
                    'work': len(workd),
                }
            }
        }
        return Response(data, status=status.HTTP_200_OK)
