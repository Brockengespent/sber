# core/views_geo.py
from datetime import datetime, timedelta
from typing import Optional, List

from django.db.models import Subquery, Max
from django.utils import timezone
from django.utils.timezone import make_aware, is_naive
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from core.models import Dog, Cs


def _parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    """
    Парсит ISO-like строку в datetime ('YYYY-MM-DD' или 'YYYY-MM-DDTHH:MM[:SS]').
    Возвращает aware datetime (с таймзоной проекта), если исходное naive.
    """
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
    """
    Пресеты периода перекрывают ручные даты, если заданы.
    """
    if period in ('7d', '30d', '90d'):
        days = int(period[:-1])
        end = timezone.now()
        start = end - timedelta(days=days)
        return start, end
    if period == 'all':
        return None, None
    return dt_from, dt_to


class HeatmapAPI(APIView):
    """
    GET /api/geo/heatmap/ (или без слеша — согласно urls)

    Параметры (все необязательные):
      - client_id: ac_client_hash (если задан, режим одного клиента)
      - period: '7d'|'30d'|'90d'|'all' (перекрывает datetime_from/to)
      - datetime_from: 'YYYY-MM-DD' или 'YYYY-MM-DDTHH:MM[:SS]'
      - datetime_to:   'YYYY-MM-DD' или 'YYYY-MM-DDTHH:MM[:SS]'
      - last_login_days: int (для режима многих клиентов — отбрасывает "давних")
      - events: повторяющийся параметр (?events=Login%20Success&events=Authorization%20Success)
      - debt_min, debt_max, bucket, npl — работают только в режиме "много клиентов"
      - limit: int (по умолчанию 20000, макс 100000)

    Ответ:
      {
        "heat_points": [[lat, lon, weight], ...],
        "count": <int>,
        "truncated": <bool>
      }
    """

    def get(self, request):
        # -------- параметры --------
        client_id = request.GET.get('client_id')

        period = request.GET.get('period')  # '7d'|'30d'|'90d'|'all'
        dt_from = _parse_iso_dt(request.GET.get('datetime_from'))
        dt_to = _parse_iso_dt(request.GET.get('datetime_to'))
        dt_from, dt_to = _apply_period(dt_from, dt_to, period)

        # список событий
        events: List[str] = request.GET.getlist('events')
        if not events:
            events = ['Login Success']

        # лимит точек
        try:
            limit = int(request.GET.get('limit', 20000))
        except Exception:
            limit = 20000
        limit = max(1000, min(limit, 100000))

        # -------- режим одного клиента --------
        if client_id:
            try:
                cid = int(client_id)
            except Exception:
                cid = client_id

            cqs = Cs.objects.filter(
                ac_client_hash=cid,
                eventaction__in=events
            )

        # -------- режим множества клиентов --------
        else:
            debt_min = request.GET.get('debt_min')
            debt_max = request.GET.get('debt_max')
            buckets = request.GET.getlist('bucket')
            npl = request.GET.get('npl')  # '0'|'1'|None
            last_login_days = request.GET.get('last_login_days')

            dqs = Dog.objects.only(
                'ac_client_hash',
                'debt_tot_os_rub_amt',
                'overdue_bucket_name',
                'npl_nflag'
            ).filter(debt_tot_os_rub_amt__gt=0)

            if debt_min not in (None, ''):
                try:
                    dqs = dqs.filter(debt_tot_os_rub_amt__gte=float(debt_min))
                except Exception:
                    pass

            if debt_max not in (None, ''):
                try:
                    dqs = dqs.filter(debt_tot_os_rub_amt__lte=float(debt_max))
                except Exception:
                    pass

            if buckets:
                dqs = dqs.filter(overdue_bucket_name__in=buckets)

            if npl in ('0', '1'):
                dqs = dqs.filter(npl_nflag=(npl == '1'))

            # отбор по давности последнего входа
            if last_login_days not in (None, ''):
                try:
                    days = int(last_login_days)
                    # MAX(dt) по cs для заданных events
                    sub = Cs.objects.filter(
                        ac_client_hash__in=dqs.values_list('ac_client_hash', flat=True),
                        eventaction__in=events
                    ).values('ac_client_hash').annotate(last_dt=Max('dt'))
                    cutoff = timezone.now() - timedelta(days=days)
                    ids = [row['ac_client_hash'] for row in sub if row['last_dt'] and row['last_dt'] >= cutoff]
                    dqs = dqs.filter(ac_client_hash__in=ids)
                except Exception:
                    pass

            client_subq = dqs.values_list('ac_client_hash', flat=True)

            cqs = Cs.objects.filter(
                ac_client_hash__in=Subquery(client_subq),
                eventaction__in=events
            )

        # -------- общие фильтры для cqs --------
        cqs = cqs.exclude(geolatitude__isnull=True).exclude(geolongitude__isnull=True) \
                 .exclude(geolatitude=0).exclude(geolongitude=0)

        # санитайзинг экстремальных координат
        cqs = cqs.filter(geolatitude__gte=-85.0, geolatitude__lte=85.0,
                         geolongitude__gte=-180.0, geolongitude__lte=180.0)

        if dt_from and dt_to:
            cqs = cqs.filter(dt__gte=dt_from, dt__lte=dt_to)
        elif dt_from and not dt_to:
            cqs = cqs.filter(dt__gte=dt_from)
        elif dt_to and not dt_from:
            cqs = cqs.filter(dt__lte=dt_to)

        cqs = cqs.order_by('-dt')[:limit]

        # Формат для Leaflet.heat: [lat, lon, weight]
        points = []
        for lat, lon in cqs.values_list('geolatitude', 'geolongitude'):
            try:
                points.append([float(lat), float(lon), 1.0])
            except Exception:
                continue

        return Response(
            {'heat_points': points, 'count': len(points), 'truncated': len(points) >= limit},
            status=status.HTTP_200_OK
        )
