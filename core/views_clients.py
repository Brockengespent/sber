from datetime import timedelta
from typing import Any, Optional

from django.utils import timezone
from django.db import connection, models
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render

from core.models import Dog, ClientCity, C, Tr, So

# ---------- Русские ярлыки категорий ----------
CATEGORY_RU = {
    'grocery': 'продукты',
    'coffee':  'кофейня',
    'ecom':    'онлайн',
    'food':    'еда',
    'p2p':     'перевод',
    'transport': 'транспорт',
    'fastfood':  'фастфуд',
    'restaurant': 'ресторан',
    'pharmacy': 'аптека',
    'entertainment': 'развлечения',
    'atm':     'банкомат',
    'fuel':    'топливо',
    None: '',
    '': '',
}

# ---------- MCC -> каноническая категория ----------
MCC_TO_CAT = {
    5411: 'grocery', 5499: 'grocery',
    5814: 'food',    5812: 'food',   5813: 'coffee',
    5969: 'ecom',    4816: 'ecom',
    5541: 'fuel',    5542: 'fuel',
    5912: 'pharmacy',
    4111: 'transport', 4121: 'transport',
    6010: 'atm',     6011: 'atm',
}

def fmt_merchant(name: Optional[Any], cat: Optional[str] = None) -> str:
    """Безопасно формирует строку 'Имя (русская категория)'."""
    try:
        nm = str(name or '—')
    except Exception:
        nm = '—'
    nm = nm.strip()
    ru = (cat or '').strip().lower()
    ru = CATEGORY_RU.get(ru, ru)
    return f"{nm} ({ru})" if ru else nm

def clean_name(raw: Optional[Any]) -> str:
    """Извлекает чистое имя из форматов:
       ['NAME', 5814, 'coffee'] | ('NAME', 5814, 'coffee') | "('NAME', 5814, 'coffee')" | "NAME (...)" | "NAME"
    """
    if isinstance(raw, (list, tuple)) and raw:
        try:
            s0 = str(raw[0])
            return s0.strip() or '—'
        except Exception:
            pass
    try:
        s = str(raw or '').strip()
    except Exception:
        s = ''
    if s.startswith("('") and "'," in s:
        try:
            inner = s[2:]
            name = inner.split("',", 1)[0]
            return name.strip() or '—'
        except Exception:
            pass
    if '(' in s and s.endswith(')'):
        return s.split('(', 1)[0].strip() or '—'
    return s or '—'

# ---------- helpers ----------
def _load_cities_safe():
    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT city
                FROM cber_schema.clients_city
                WHERE city IS NOT NULL AND city <> ''
                ORDER BY city
            """)
            return [r[0] for r in cur.fetchall()]
    except Exception:
        return []

def _period_range(period: str):
    now = timezone.now()
    if period == '7d':
        return now - timedelta(days=7), now
    if period == '90d':
        return now - timedelta(days=90), now
    if period == 'all':
        return None, None
    return now - timedelta(days=30), now

# ---------- Список клиентов (без дублей) ----------
def clients_table_view(request):
    cities = _load_cities_safe()
    selected_city = (request.GET.get('city') or '').strip()
    debt_min = request.GET.get('debt_min')
    debt_max = request.GET.get('debt_max')
    buckets = request.GET.getlist('bucket')

    where_parts, params = [], []
    if debt_min not in (None, ''):
        where_parts.append('dog.debt_tot_os_rub_amt >= %s')
        params.append(debt_min)
    if debt_max not in (None, ''):
        where_parts.append('dog.debt_tot_os_rub_amt <= %s')
        params.append(debt_max)
    if buckets:
        where_parts.append('dog.overdue_bucket_name = ANY(%s)')
        params.append(buckets)
    if selected_city and (not cities or selected_city in cities):
        where_parts.append("""
            dog.ac_client_hash IN (
                SELECT ac_client_hash
                FROM cber_schema.clients_city
                WHERE city = %s
            )
        """)
        params.append(selected_city)

    where_sql = ('WHERE ' + ' AND '.join(where_parts)) if where_parts else ''

    base_sql = f"""
    WITH ranked AS (
        SELECT
            dog.id,
            dog.ac_client_hash,
            dog.debt_tot_os_rub_amt,
            dog.overdue_bucket_name,
            dog.npl_nflag,
            ROW_NUMBER() OVER (
                PARTITION BY dog.ac_client_hash
                ORDER BY dog.debt_tot_os_rub_amt DESC, dog.id DESC
            ) AS rn
        FROM core_dog AS dog
        {where_sql}
    ),
    best AS (
        SELECT
            id, ac_client_hash, debt_tot_os_rub_amt, overdue_bucket_name, npl_nflag
        FROM ranked
        WHERE rn = 1
    )
    SELECT
        b.id, b.ac_client_hash, b.debt_tot_os_rub_amt, b.overdue_bucket_name, b.npl_nflag, cc.city
    FROM best b
    LEFT JOIN LATERAL (
        SELECT city
        FROM cber_schema.clients_city cc
        WHERE cc.ac_client_hash = b.ac_client_hash
        ORDER BY city ASC
        LIMIT 1
    ) cc ON TRUE
    """

    ordering = request.GET.get('ordering', 'overdue_desc')
    if ordering == 'overdue_asc':
        order_sql = """
        ORDER BY
        CASE b.overdue_bucket_name
            WHEN '0' THEN 0 WHEN '1-30' THEN 30 WHEN '30-60' THEN 60
            WHEN '60-90' THEN 90 WHEN '90-120' THEN 120 WHEN '120-180' THEN 180
            WHEN '180+' THEN 999 ELSE 0 END ASC, b.id ASC
        """
    elif ordering == 'total_debt':
        order_sql = 'ORDER BY b.debt_tot_os_rub_amt ASC, b.id ASC'
    elif ordering == '-total_debt':
        order_sql = 'ORDER BY b.debt_tot_os_rub_amt DESC, b.id DESC'
    else:
        order_sql = """
        ORDER BY
        CASE b.overdue_bucket_name
            WHEN '0' THEN 0 WHEN '1-30' THEN 30 WHEN '30-60' THEN 60
            WHEN '60-90' THEN 90 WHEN '90-120' THEN 120 WHEN '120-180' THEN 180
            WHEN '180+' THEN 999 ELSE 0 END DESC, b.id DESC
        """

    count_sql = f"""
    WITH ranked AS (
        SELECT dog.ac_client_hash,
        ROW_NUMBER() OVER (PARTITION BY dog.ac_client_hash
                           ORDER BY dog.debt_tot_os_rub_amt DESC, dog.id DESC) AS rn
        FROM core_dog AS dog
        {where_sql}
    )
    SELECT COUNT(*) FROM ranked WHERE rn = 1
    """

    try:
        page = int(request.GET.get('page', 1))
    except (TypeError, ValueError):
        page = 1
    try:
        page_size = int(request.GET.get('page_size', 50))
    except (TypeError, ValueError):
        page_size = 50
    page = max(page, 1); page_size = max(page_size, 1)
    offset = (page - 1) * page_size

    with connection.cursor() as cur:
        cur.execute(count_sql, params)
        total_count = cur.fetchone()[0]

    with connection.cursor() as cur:
        cur.execute(f"{base_sql} {order_sql} LIMIT %s OFFSET %s", params + [page_size, offset])
        rows = cur.fetchall()

    results = [{
        'id': r[0],
        'client_id': r[1],
        'total_debt': r[2],
        'overdue_bucket': r[3],
        'npl_nflag': bool(r[4]),
        'city': r[5] or '—',
    } for r in rows]

    def page_url(p: int) -> str:
        q = request.GET.copy(); q['page'] = p
        return f"{request.path}?{q.urlencode()}"

    last_page = (total_count + page_size - 1) // page_size
    next_url = page_url(page + 1) if page < last_page else None
    prev_url = page_url(page - 1) if page > 1 else None

    context = {
        'results': results, 'count': total_count,
        'next': next_url, 'previous': prev_url,
        'cities': cities, 'selected_city': selected_city, 'ordering': ordering,
    }
    return render(request, 'core/partials/clients_table.html', context)

def buckets_list_api(request):
    with connection.cursor() as cur:
        cur.execute("SELECT DISTINCT overdue_bucket_name FROM cber_schema.core_dog;")
        rows = [r[0] for r in cur.fetchall() if r and r is not None]
    buckets = sorted(set(rows), key=lambda x: (x != '0', str(x)))
    return JsonResponse({'buckets': buckets})

# ---------- Детальная страница клиента ----------
def client_detail_view(request, pk: int):
    from django.db import models as dj_models
    from django.db.models import OuterRef, Subquery, F, Value, Sum, Count, Case, When
    from django.db.models.functions import Coalesce, Cast
    from django.db.models import DateTimeField

    obj = get_object_or_404(Dog, pk=pk)
    period = request.GET.get('period', 'all')
    dt_from, dt_to = _period_range(period)

    city = ClientCity.objects.filter(ac_client_hash=obj.ac_client_hash).values_list('city', flat=True).first()

    # Соответствие So ↔ C
    operations_qs = So.objects.filter(
        ac_client_hash=OuterRef('ac_client_hash'),
        oper_rur_amt=OuterRef('c_txn_rub_amt'),
        date_time_oper__date=OuterRef('c_txn_dt__date')
    ).order_by('date_time_oper')

    # Поступления (C)
    inc_qs = C.objects.filter(ac_client_hash=obj.ac_client_hash)
    if dt_from and dt_to:
        inc_qs = inc_qs.filter(c_txn_dt__gte=dt_from, c_txn_dt__lte=dt_to)

    inc_qs_in = inc_qs.filter(c_txn_rub_amt__gt=0).annotate(
        real_datetime=Coalesce(Subquery(operations_qs.values('date_time_oper')[:1]), F('c_txn_dt')),
        source=Coalesce('pmnt_payer_name', Subquery(operations_qs.values('doc_type')[:1]), Value('—')),
    )

    inc_agg = inc_qs_in.aggregate(total=Sum('c_txn_rub_amt'), count=Count('*'))
    inc_latest = list(
        inc_qs_in.order_by('-real_datetime').values('real_datetime', 'source', 'txn_cod_type_name', 'c_txn_rub_amt')[:10]
    )

    weeks_map = {'7d': 1, '30d': 4, '90d': 13}
    weeks = weeks_map.get(period)
    inc_avg_week = (inc_agg['total'] / weeks) if (weeks and inc_agg.get('total')) else None

    # Общие настройки фильтров таблицы
    tx_date_ordering = request.GET.get('tx_date_ordering', '-date')
    tx_direction = (request.GET.get('tx_direction') or '').upper()
    try:
        tx_page = int(request.GET.get('tx_page', 1))
    except (TypeError, ValueError):
        tx_page = 1
    try:
        tx_page_size = int(request.GET.get('tx_page_size', 50))
    except (TypeError, ValueError):
        tx_page_size = 50

    # Нормализация входящих в общую схему
    c_qs = C.objects.filter(ac_client_hash=obj.ac_client_hash)
    if dt_from and dt_to:
        c_qs = c_qs.filter(c_txn_dt__gte=dt_from, c_txn_dt__lte=dt_to)
    c_qs = c_qs.filter(c_txn_rub_amt__gt=0)
    income_rows = c_qs.annotate(
        date=Cast('c_txn_dt', output_field=DateTimeField()),
        amount=F('c_txn_rub_amt'),
        city=Value('—', output_field=dj_models.CharField()),
        merchant=Coalesce('pmnt_payer_name', Value('—')),
        merchant_cat=Value('', output_field=dj_models.CharField()),
        direction=Value('C', output_field=dj_models.CharField()),
    ).values('date', 'amount', 'city', 'merchant', 'merchant_cat', 'direction')

    # Карточные операции (Tr)
    tr_qs = Tr.objects.filter(ac_client_hash=obj.ac_client_hash)
    if dt_from and dt_to:
        tr_qs = tr_qs.filter(c_txn_dt__gte=dt_from, c_txn_dt__lte=dt_to)

    # Категория по MCC (t_mcc_code)
    merchant_cat_value = Case(
        When(txn_cod_type_rk=5411, then=Value('grocery')),
        When(txn_cod_type_rk=5499, then=Value('grocery')),
        When(txn_cod_type_rk=5814, then=Value('food')),
        When(txn_cod_type_rk=5812, then=Value('food')),
        When(txn_cod_type_rk=5813, then=Value('coffee')),
        When(txn_cod_type_rk=5969, then=Value('ecom')),
        When(txn_cod_type_rk=4816, then=Value('ecom')),
        When(txn_cod_type_rk=5541, then=Value('fuel')),
        When(txn_cod_type_rk=5542, then=Value('fuel')),
        When(txn_cod_type_rk=5912, then=Value('pharmacy')),
        When(txn_cod_type_rk=4111, then=Value('transport')),
        When(txn_cod_type_rk=4121, then=Value('transport')),
        When(txn_cod_type_rk=6010, then=Value('atm')),
        When(txn_cod_type_rk=6011, then=Value('atm')),
        default=Value(''),
        output_field=dj_models.CharField()
    )


    card_rows = tr_qs.annotate(
        date=Cast('c_txn_dt', output_field=DateTimeField()),
        amount=F('c_txn_rub_amt'),
        city=Coalesce('t_trx_city', Value('—')),
        merchant=Coalesce('t_merchant_name', Value('—')),
        merchant_cat=merchant_cat_value,
        direction=Coalesce('t_trx_direction', Value('')),
    ).values('date', 'amount', 'city', 'merchant', 'merchant_cat', 'direction')

    # Объединение и фильтрация
    from itertools import chain
    combined = list(chain(income_rows, card_rows))
    if tx_direction in ('C', 'D'):
        combined = [r for r in combined if (r.get('direction') or '') == tx_direction]

    reverse = (tx_date_ordering != 'date')
    combined.sort(key=lambda r: r.get('date') or timezone.make_aware(timezone.datetime.min), reverse=reverse)

    # Пагинация
    from django.core.paginator import Paginator
    tx_paginator = Paginator(combined, tx_page_size)
    tx_page_obj = tx_paginator.get_page(tx_page)

    def tx_page_url(p):
        params = request.GET.copy()
        params['tx_page'] = p
        params['tx_page_size'] = tx_page_size
        return f"{request.path}?{params.urlencode()}"

    # Готовые строки для шаблона — только merchant_display
    tx_rows = []
    for r in tx_page_obj.object_list:
        raw_merchant = r.get('merchant')
        name_clean = clean_name(raw_merchant)
        cat = r.get('merchant_cat') or ''
        display = fmt_merchant(name_clean, cat)
        tx_rows.append({
            'c_txn_dt': r['date'],
            'c_txn_rub_amt': r['amount'],
            't_trx_city': r['city'],
            'merchant_display': display,
            'direction': r.get('direction'),
        })

    # Топ мерчантов по расходам (D)
    merch_qs = Tr.objects.filter(ac_client_hash=obj.ac_client_hash)
    if dt_from and dt_to:
        merch_qs = merch_qs.filter(c_txn_dt__gte=dt_from, c_txn_dt__lte=dt_to)
    merch_qs = merch_qs.filter(t_trx_direction='D', c_txn_rub_amt__gt=0)

    merchant_field = Coalesce('t_merchant_name', Value('—'))
    top_limit = 5
    merch_agg = (
        merch_qs.values(name=merchant_field)
        .annotate(amount=Sum('c_txn_rub_amt'), ops=Count('*'))
        .order_by('-amount')[:top_limit]
    )

    out_total = merch_qs.aggregate(total=Sum('c_txn_rub_amt'))['total'] or 0
    top_merchants = []
    for m in merch_agg:
        amt = m['amount'] or 0
        share = float(amt) / float(out_total) * 100 if out_total else 0.0
        top_merchants.append({'name': fmt_merchant(m['name']), 'amount': amt, 'ops': m['ops'], 'share': share})

    # ------ Мини‑сводки по поступлениям ------
    peak_day = None; peak_amt = None
    with connection.cursor() as cur:
        cur.execute(f"""
            SELECT DATE(c.c_txn_dt) AS d, SUM(c.c_txn_rub_amt) AS s
            FROM c AS c
            WHERE c."ac.client_hash" = %s AND c.c_txn_rub_amt > 0
            {("AND c.c_txn_dt >= %s AND c.c_txn_dt <= %s" if (dt_from and dt_to) else "")}
            GROUP BY d ORDER BY s DESC LIMIT 1
        """, [obj.ac_client_hash] + ([dt_from, dt_to] if (dt_from and dt_to) else []))
        row = cur.fetchone()
        if row: peak_day, peak_amt = row[0], row[1]

    median_amt = None; p90_amt = None
    with connection.cursor() as cur:
        cur.execute(f"""
            SELECT
                percentile_disc(0.5) WITHIN GROUP (ORDER BY c.c_txn_rub_amt),
                percentile_disc(0.9) WITHIN GROUP (ORDER BY c.c_txn_rub_amt)
            FROM c AS c
            WHERE c."ac.client_hash" = %s AND c.c_txn_rub_amt > 0
            {("AND c.c_txn_dt >= %s AND c.c_txn_dt <= %s" if (dt_from and dt_to) else "")}
        """, [obj.ac_client_hash] + ([dt_from, dt_to] if (dt_from and dt_to) else []))
        row = cur.fetchone()
        if row: median_amt, p90_amt = row[0], row[1]

    active_days = 0; max_gap_days = 0
    with connection.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT DATE(c.c_txn_dt) AS d
            FROM c AS c
            WHERE c."ac.client_hash" = %s AND c.c_txn_rub_amt > 0
            {("AND c.c_txn_dt >= %s AND c.c_txn_dt <= %s" if (dt_from and dt_to) else "")}
            ORDER BY d
        """, [obj.ac_client_hash] + ([dt_from, dt_to] if (dt_from and dt_to) else []))
        raw_rows = cur.fetchall()
    days = []
    for row in raw_rows:
        d = row[0]
        try: d = d.date()
        except Exception: pass
        days.append(d)
    active_days = len(days)
    prev = None
    for d in days:
        if prev is not None:
            try: gap = (d - prev).days - 1
            except Exception: gap = 0
            if gap > max_gap_days: max_gap_days = gap
        prev = d

    total_in_period = inc_agg.get('total') or 0
    top_sources = []
    with connection.cursor() as cur:
        cur.execute(f"""
            SELECT source, SUM(amount) AS s
            FROM (
                SELECT COALESCE(c.pmnt_payer_name, '—') AS source, c.c_txn_rub_amt AS amount
                FROM c AS c
                WHERE c."ac.client_hash" = %s AND c.c_txn_rub_amt > 0
                {("AND c.c_txn_dt >= %s AND c.c_txn_dt <= %s" if (dt_from and dt_to) else "")}
            ) t
            GROUP BY source ORDER BY s DESC LIMIT 3
        """, [obj.ac_client_hash] + ([dt_from, dt_to] if (dt_from and dt_to) else []))
        rows = cur.fetchall()
    for name, s in rows:
        share = float(s) / float(total_in_period) * 100 if total_in_period else 0.0
        top_sources.append({'name': fmt_merchant(name), 'amount': s, 'share': share})

    # ------ Профиль трат/гео/ATM ------
    spend_total = out_total
    spend_count = merch_qs.count()
    avg_check = float(spend_total) / spend_count if spend_count else 0.0

    weekday_map = ['Вс', 'Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб']
    weekday_peak = None; weekday_peak_share = 0.0
    with connection.cursor() as cur:
        cur.execute(f"""
            SELECT EXTRACT(DOW FROM t.t_evt_posted_dttm)::int AS dow, SUM(t.t_amt) AS s
            FROM tr AS t
            WHERE t."t_client_hash" = %s AND t.t_trx_direction = 'D' AND t.t_amt > 0
            {("AND t.t_evt_posted_dttm >= %s AND t.t_evt_posted_dttm <= %s" if (dt_from and dt_to) else "")}
            GROUP BY dow ORDER BY s DESC LIMIT 1
        """, [obj.ac_client_hash] + ([dt_from, dt_to] if (dt_from and dt_to) else []))
        r = cur.fetchone()
    if r:
        dow = int(r[0]); weekday_peak = weekday_map[dow] if 0 <= dow <= 6 else str(dow)
        weekday_peak_sum = r[1]
        weekday_peak_share = float(weekday_peak_sum) / float(spend_total) * 100 if spend_total else 0.0

    top_city = None
    with connection.cursor() as cur:
        cur.execute(f"""
            SELECT t.t_trx_city, SUM(t.t_amt) AS s
            FROM tr AS t
            WHERE t."t_client_hash" = %s AND t.t_trx_direction = 'D' AND t.t_amt > 0
            {("AND t.t_evt_posted_dttm >= %s AND t.t_evt_posted_dttm <= %s" if (dt_from and dt_to) else "")}
            GROUP BY t.t_trx_city
            HAVING t.t_trx_city IS NOT NULL AND t.t_trx_city <> '' AND t.t_trx_city <> '—'
            ORDER BY s DESC LIMIT 1
        """, [obj.ac_client_hash] + ([dt_from, dt_to] if (dt_from and dt_to) else []))
        r = cur.fetchone()
    if r: top_city = r[0]

    geo_qs = Tr.objects.filter(ac_client_hash=obj.ac_client_hash)
    if dt_from and dt_to:
        geo_qs = geo_qs.filter(c_txn_dt__gte=dt_from, c_txn_dt__lte=dt_to)
    geo_qs = geo_qs.filter(t_trx_direction='D')
    geo_total = geo_qs.count()
    geo_with_city = geo_qs.exclude(t_trx_city__isnull=True).exclude(t_trx_city='').exclude(t_trx_city='—').count()
    geo_share = float(geo_with_city) / float(geo_total) * 100 if geo_total else 0.0

    atm_qs = Tr.objects.filter(
        ac_client_hash=obj.ac_client_hash,
        t_trx_direction='D',
        c_txn_rub_amt__gt=0,
    )
    if dt_from and dt_to:
        atm_qs = atm_qs.filter(c_txn_dt__gte=dt_from, c_txn_dt__lte=dt_to)
    atm_qs = atm_qs.filter(t_merchant_name__icontains='ATM')
    atm_sum = atm_qs.aggregate(s=Sum('c_txn_rub_amt'))['s'] or 0
    atm_share = float(atm_sum) / float(spend_total) * 100 if spend_total else 0.0

    context = {
        'obj': obj, 'period': period, 'city': city or '—',
        'inc_total': inc_agg.get('total'), 'inc_count': inc_agg.get('count') or 0,
        'inc_latest': inc_latest, 'inc_avg_week': inc_avg_week,
        'tx_rows': tx_rows, 'tx_count': tx_paginator.count,
        'tx_next': tx_page_url(tx_page_obj.next_page_number()) if tx_page_obj.has_next() else None,
        'tx_prev': tx_page_url(tx_page_obj.previous_page_number()) if tx_page_obj.has_previous() else None,
        'tx_date_ordering': tx_date_ordering, 'tx_direction': tx_direction, 'tx_page_size': tx_page_size,
        'top_merchants': top_merchants, 'out_total': spend_total,
        'inc_peak_day': peak_day, 'inc_peak_amt': peak_amt,
        'inc_median': median_amt, 'inc_p90': p90_amt,
        'inc_active_days': active_days, 'inc_max_gap_days': max_gap_days, 'inc_top_sources': top_sources,
        'spend_total': spend_total, 'spend_count': spend_count, 'avg_check': avg_check,
        'weekday_peak': weekday_peak, 'weekday_peak_share': weekday_peak_share,
        'geo_share': geo_share, 'top_city': top_city,
        'atm_sum': atm_sum, 'atm_share': atm_share,
    }
    return render(request, 'core/client_detail.html', context)

def client_heatmap_view(request, pk: int):
    obj = get_object_or_404(Dog, pk=pk)
    return render(request, 'core/client_heatmap.html', {'obj': obj})
