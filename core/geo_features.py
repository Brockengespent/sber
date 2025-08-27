# core/geo_features.py
from datetime import timedelta
from typing import Optional, List, Dict, Any, Tuple
from django.utils import timezone
from django.db.models import Q, Max
from .models import Cs

DIGITS = 4  # округление координат до ~11м
RADIUS_M_DEFAULT = 300

def _round_coord(v: float, digits: int = DIGITS) -> float:
    return round(float(v), digits)

def _in_night(dt) -> bool:
    loc = dt.astimezone(timezone.get_current_timezone())
    return loc.hour >= 21 or loc.hour < 8

def _in_workday(dt) -> bool:
    loc = dt.astimezone(timezone.get_current_timezone())
    return loc.weekday() < 5 and 9 <= loc.hour < 18

def period_range(period: Optional[str]) -> Tuple[Optional[timezone.datetime], Optional[timezone.datetime]]:
    if period in ('7d', '30d', '90d'):
        days = int(period[:-1])
        end = timezone.now()
        start = end - timedelta(days=days)
        return start, end
    if period == 'all':
        return None, None
    # по умолчанию 30 дней
    end = timezone.now()
    return end - timedelta(days=30), end

def load_events_qs(client_id: str, events: Optional[List[str]] = None,
                   period: Optional[str] = '30d'):
    events = events or ['Login Success', 'Authorization Success']
    dt_from, dt_to = period_range(period)

    qs = (Cs.objects.filter(
            ac_client_hash=str(client_id),
            eventaction__in=events
        )
        .exclude(geolatitude__isnull=True).exclude(geolongitude__isnull=True)
        .exclude(geolatitude=0).exclude(geolongitude=0)
        .filter(geolatitude__gte=-85.0, geolatitude__lte=85.0,
                geolongitude__gte=-180.0, geolongitude__lte=180.0)
    )
    if dt_from and dt_to:
        qs = qs.filter(dt__gte=dt_from, dt__lte=dt_to)
    elif dt_from:
        qs = qs.filter(dt__gte=dt_from)
    elif dt_to:
        qs = qs.filter(dt__lte=dt_to)
    return qs

def most_frequent_cell(rows: List[Dict[str, Any]], label: str) -> Optional[Dict[str, Any]]:
    if not rows:
        return None
    cells: Dict[tuple, int] = {}
    last_seen: Dict[tuple, Any] = {}
    for r in rows:
        key = (_round_coord(r['lat']), _round_coord(r['lon']))
        cells[key] = cells.get(key, 0) + 1
        if key not in last_seen or r['dt'] > last_seen[key]:
            last_seen[key] = r['dt']
    key_max = max(cells, key=lambda k: cells[k])
    size_max = cells[key_max]
    total = len(rows)
    share = size_max / total if total else 0.0
    return {
        "type": label,
        "lat": key_max[0],
        "lon": key_max[1],
        "radius_m": RADIUS_M_DEFAULT,
        "confidence": share,
        "size": size_max,
        "share": share,
        "last_seen": last_seen.get(key_max).date().isoformat() if last_seen.get(key_max) else None
    }

def compute_home_work_and_activity(client_id: str, period: str = '30d',
                                   events: Optional[List[str]] = None):
    qs = load_events_qs(client_id, events=events, period=period).order_by('-dt')
    rows_qs = list(qs.values('dt', 'geolatitude', 'geolongitude'))

    night, workd = [], []
    hourly =  [0]* 24
    weekday = [0] * 7

    tz = timezone.get_current_timezone()
    for r in rows_qs:
        dt = r['dt'].astimezone(tz)
        lat = float(r['geolatitude']); lon = float(r['geolongitude'])
        hourly[dt.hour] += 1
        weekday[dt.weekday()] += 1
        item = {'dt': dt, 'lat': lat, 'lon': lon}
        if _in_night(dt):
            night.append(item)
        if _in_workday(dt):
            workd.append(item)

    home = most_frequent_cell(night, 'home')
    work = most_frequent_cell(workd, 'work')

    # Слабая фильтрация: требуем минимум 3 точки в кластере
    if home and home.get('size', 0) < 2:
        home = None
    if work and work.get('size', 0) < 2:
        work = None

    return {
        "places": [p for p in [home, work] if p],
        "activity": {"hourly": hourly, "weekday": weekday},
        "counts": {"total": len(rows_qs), "night": len(night), "work": len(workd)},
    }
