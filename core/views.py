from importlib.metadata import files  # можно убрать
import os
import mimetypes
import pandas as pd
import pytz

from django.conf import settings
from django.core.files.storage import FileSystemStorage
from django.http import FileResponse, Http404
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.http import require_http_methods
from django.db import connection

from .models import Cs, C, Tr, So, Dog

# -------------------- Глобальные утилиты --------------------

moscow_tz = pytz.timezone("Europe/Moscow")

def to_aware(df, cols):
    """Переводит указанные колонки df в aware datetime по Europe/Moscow."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].apply(
                lambda v: v if (pd.isna(v) or timezone.is_aware(v))
                else timezone.make_aware(v, moscow_tz)
            )

def table_count(model):
    return model.objects.count()

def _coerce_bool(val):
    if pd.isna(val):
        return False
    if isinstance(val, (int, float)):
        return bool(int(val))
    if isinstance(val, str):
        s = val.strip().lower()
        if s in ('1','true','t','yes','y'): return True
        if s in ('0','false','f','no','n',''): return False
    return bool(val)

def _coerce_str(val):
    if pd.isna(val):
        return ''
    try:
        if isinstance(val, int): return str(val)
        if isinstance(val, float): return str(int(val))
        return str(val).strip()
    except Exception:
        return str(val)

def as_int_or_none(v):
    import pandas as pd
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return int(v)
    except Exception:
        return None


def as_dt_or_none(v):
    import pandas as pd
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    ts = pd.to_datetime(v, errors='coerce')
    if pd.isna(ts):
        return None
    try:
        ts = ts.tz_localize(None)
    except Exception:
        try:
            ts = ts.tz_convert(None)
        except Exception:
            pass
    return ts.to_pydatetime() if hasattr(ts, 'to_pydatetime') else ts

def as_date_or_none(v):
    import pandas as pd
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    d = pd.to_datetime(v, errors='coerce')
    if pd.isna(d):
        return None
    try:
        return d.date()
    except Exception:
        return None



# -------------------- Страницы --------------------

def index(request):
    return render(request, 'core/index.html')

def clients_page(request):
    return render(request, 'core/clients.html')

def _download_static_file(rel_path: str, download_name: str):
    abs_path = os.path.join(settings.BASE_DIR, rel_path)
    if not os.path.exists(abs_path):
        raise Http404("Template not found")
    content_type, _ = mimetypes.guess_type(abs_path)
    resp = FileResponse(open(abs_path, 'rb'), as_attachment=True, filename=download_name)
    if content_type:
        resp['Content-Type'] = content_type
    return resp

def download_template_cs(request): return _download_static_file('core/static/core/template_cs.xlsx', 'template_cs.xlsx')
def download_template_c(request):  return _download_static_file('core/static/core/template_c.xlsx',  'template_c.xlsx')
def download_template_tr(request): return _download_static_file('core/static/core/template_tr.xlsx', 'template_tr.xlsx')
def download_template_so(request): return _download_static_file('core/static/core/template_so.xlsx', 'template_so.xlsx')
def download_template_dog(request): return _download_static_file('core/static/core/template_dog.xlsx','template_dog.xlsx')

# -------------------- Новая страница загрузки 5 файлов --------------------

@require_http_methods(["GET", "POST"])
def upload_multi_page(request):
    messages = []

    if request.method == "GET":
        return render(request, 'core/upload_multi.html', {'messages': messages})

    clear_flag = request.POST.get('clear') == '1'
    files = {
        'cs': request.FILES.get('file_cs'),
        'c':  request.FILES.get('file_c'),
        'tr': request.FILES.get('file_tr'),
        'so': request.FILES.get('file_so'),
        'dog':request.FILES.get('file_dog'),
    }

    # Очистка только тех таблиц, по которым пришли файлы
    try:
        if clear_flag:
            if files['cs']: Cs.objects.all().delete()
            if files['c']:  C.objects.all().delete()
            if files['tr']: Tr.objects.all().delete()
            if files['so']: So.objects.all().delete()
            if files['dog']:Dog.objects.all().delete()
            messages.append("Таблицы очищены для загружаемых сущностей.")
    except Exception as e:
        messages.append(f"Ошибка очистки: {e}")

    def read_excel(uploaded_file, dtype=None):
        try:
            return pd.read_excel(uploaded_file, engine='openpyxl', dtype=dtype or {})
        except Exception as e:
            messages.append(f"Ошибка чтения {getattr(uploaded_file, 'name', '<file>')}: {e}")
            return None

    # ---- Cs (RAW SQL, т.к. нет PK/id) ----
    if files['cs']:
        df = read_excel(files['cs'], dtype={'ac.client_hash': str})
        if df is not None:
            df = df.dropna(how='all').copy()
            df.columns = [str(c).strip().replace('.', '_') for c in df.columns]

            if 'dt' in df.columns:
                df['dt'] = pd.to_datetime(df['dt'], errors='coerce')
                df['dt'] = df['dt'].apply(lambda v: v if (pd.isna(v) or timezone.is_aware(v)) else timezone.make_aware(v, moscow_tz))

            dp_series = None
            if 'date_part' in df.columns:
                dp_series = pd.to_datetime(df['date_part'], errors='coerce').dt.date

            rows = []
            for i, row in df.iterrows():
                ac = (str(row.get('ac_client_hash') or '')).strip()
                eventaction = row.get('eventaction')
                geolat = row.get('geolatitude')
                geolon = row.get('geolongitude')
                dt_val = row.get('dt')
                dp_val = None
                if isinstance(dp_series, pd.Series):
                    raw_dp = dp_series.iloc[i]
                    dp_val = None if pd.isna(raw_dp) else raw_dp
                rows.append((ac, eventaction, geolat, geolon, dt_val, dp_val))

            if rows:
                with connection.cursor() as cur:
                    cur.executemany(
                        'INSERT INTO cber_schema.cs ("ac.client_hash", eventaction, geolatitude, geolongitude, dt, date_part) VALUES (%s,%s,%s,%s,%s,%s)',
                        rows
                    )
                messages.append(f"Cs: добавлено {len(rows)} (RAW SQL)")

    # ---- C (RAW SQL: устойчиво к формату дат) ----
    if files['c']:
        df = read_excel(files['c'], dtype={'ac.client_hash': str})
        if df is None:
            messages.append("C: файл не прочитан (пропущено)")
        else:
            df = df.dropna(how='all').copy()
            df.columns = [str(c).strip().replace('.', '_') for c in df.columns]

            # Числовые NaN -> None
            for col in ('txn_cod_type_rk', 'c_txn_rub_amt'):
                if col in df.columns:
                    df[col] = df[col].where(~pd.isna(df[col]), None)

            rows = []
            bad_rows = 0
            for _, row in df.iterrows():
                c_src = (row.get('src') or None)
                c_ach = (str(row.get('ac_client_hash') or '').strip() or None)

                raw_dt = row.get('c_txn_dt')
                c_dt  = as_dt_or_none(raw_dt)  # python datetime или None
                if c_dt is None and (raw_dt not in (None, '', 'NaT')):
                    bad_rows += 1
                    continue

                rk = row.get('txn_cod_type_rk')
                try:
                    rk = int(rk) if rk is not None and not pd.isna(rk) else None
                except Exception:
                    rk = None

                rk_name = (row.get('txn_cod_type_name') or None)

                amt = row.get('c_txn_rub_amt')
                if isinstance(amt, float) and pd.isna(amt):
                    amt = None

                payer = (row.get('pmnt_payer_name') or None)

                dp = as_date_or_none(row.get('day_part'))  # date или None

                rows.append((c_src, c_ach, c_dt, rk, rk_name, amt, payer, dp))

            if rows:
                try:
                    with connection.cursor() as cur:
                        # Имена столбцов как в таблице c (из твоей модели: db_table='c')
                        cur.executemany(
                            'INSERT INTO c (src, "ac.client_hash", c_txn_dt, txn_cod_type_rk, '
                            'txn_cod_type_name, c_txn_rub_amt, pmnt_payer_name, day_part) '
                            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
                            rows
                        )
                    msg = f"C: добавлено {len(rows)} (RAW SQL)"
                    if bad_rows:
                        msg += f", пропущено {bad_rows} строк с некорректной c_txn_dt"
                    messages.append(msg)
                except Exception as e:
                    messages.append(f"C: ошибка вставки (RAW SQL): {e}")
            else:
                messages.append("C: нет валидных строк для вставки (после фильтра дат)")


    # ---- Tr (RAW SQL: без ORM, устойчиво к NaT/tz) ----
    if files['tr']:
        df = read_excel(files['tr'], dtype={'ac_client_hash': str})
        if df is None:
            messages.append("Tr: файл не прочитан (пропущено)")
        else:
            df = df.dropna(how='all').copy()
            df.columns = [str(c).strip().replace('.', '_') for c in df.columns]

            # адаптер старого шаблона → к нужным
            if 't_mcc_code' not in df.columns and 'txn_cod_type_rk' in df.columns:
                df['t_mcc_code'] = df['txn_cod_type_rk']
            if 't_merchant_name' not in df.columns and 'pmnt_payer_name' in df.columns:
                df['t_merchant_name'] = df['pmnt_payer_name']
            if 't_trx_direction' not in df.columns:
                df['t_trx_direction'] = 'D'
            for col in ('t_trx_city', 't_trans_type', 't_merchant_id', 't_terminal_id'):
                if col not in df.columns:
                    df[col] = None

            # числовые NaN → None
            for col in ('t_mcc_code', 't_trans_type', 'c_txn_rub_amt'):
                if col in df.columns:
                    df[col] = df[col].where(~pd.isna(df[col]), None)

            rows = []
            bad_rows = 0
            for _, row in df.iterrows():
                t_src           = (row.get('src') or None)
                t_client_hash   = (str(row.get('ac_client_hash') or '').strip() or None)

                raw_dt          = row.get('c_txn_dt')
                t_evt_posted    = as_dt_or_none(raw_dt)  # python datetime или None
                if t_evt_posted is None and (raw_dt not in (None, '', 'NaT')):
                    bad_rows += 1
                    continue

                t_trx_city      = (row.get('t_trx_city') or None)

                t_mcc_code = None
                if row.get('t_mcc_code') is not None:
                    try:
                        t_mcc_code = int(row.get('t_mcc_code'))
                    except Exception:
                        t_mcc_code = None

                t_trans_type = None
                if row.get('t_trans_type') is not None:
                    try:
                        t_trans_type = int(row.get('t_trans_type'))
                    except Exception:
                        t_trans_type = None

                t_trx_direction = (str(row.get('t_trx_direction') or '').strip().upper()[:1] or None)
                t_merchant_id   = (row.get('t_merchant_id') or None)
                t_terminal_id   = (row.get('t_terminal_id') or None)
                t_merchant_name = (row.get('t_merchant_name') or None)

                t_amt = row.get('c_txn_rub_amt')
                if isinstance(t_amt, float) and pd.isna(t_amt):
                    t_amt = None

                day_part_date = as_date_or_none(row.get('day_part'))

                rows.append((
                    t_src, t_client_hash, t_evt_posted, t_trx_city,
                    t_mcc_code, t_trans_type, t_trx_direction,
                    t_merchant_id, t_terminal_id, t_merchant_name,
                    t_amt, day_part_date
                ))

            if rows:
                try:
                    with connection.cursor() as cur:
                        # ВНИМАНИЕ: t.src с точкой — экранируем кавычками
                        cur.executemany(
                            'INSERT INTO tr (t_src, t_client_hash, t_evt_posted_dttm, t_trx_city, '
                            't_mcc_code, t_trans_type, t_trx_direction, t_merchant_id, t_terminal_id, '
                            't_merchant_name, t_amt, day_part) '
                            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                            rows
                        )

                    msg = f"Tr: добавлено {len(rows)} (RAW SQL)"
                    if bad_rows:
                        msg += f", пропущено {bad_rows} строк с некорректной c_txn_dt"
                    messages.append(msg)
                except Exception as e:
                    messages.append(f"Tr: ошибка вставки (RAW SQL): {e}")
            else:
                messages.append("Tr: нет валидных строк для вставки (после фильтра дат)")



    # ---- So (RAW SQL для схемы: bigint, numeric, smallint) ----
    # Требуются утилиты: as_dt_or_none, as_date_or_none, to_bigint_or_none, to_smallint_or_none, to_decimal_or_none
    from decimal import Decimal

    def to_bigint_or_none(v):
        import pandas as pd
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        try:
            x = int(str(v).strip())
            if x < -9223372036854775808 or x > 9223372036854775807:
                return None
            return x
        except Exception:
            return None

    def to_smallint_or_none(v):
        import pandas as pd
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        try:
            x = int(str(v).strip())
            if x < -32768 or x > 32767:
                return None
            return x
        except Exception:
            return None

    def to_decimal_or_none(v):
        import pandas as pd
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        try:
            return Decimal(str(v))
        except Exception:
            return None

    # Вставить внутрь upload_multi_page вместо текущего блока SO:
    # ---- So (RAW SQL) ----
    if files['so']:
        df = read_excel(files['so'], dtype={'ac.client_hash': str, 't.p2p_flg': str})
        if df is None:
            messages.append("So: файл не прочитан (пропущено)")
        else:
            df = df.dropna(how='all').copy()
            df.columns = [str(c).strip().replace('.', '_') for c in df.columns]

            rows = []
            bad_dt = 0
            for _, row in df.iterrows():
                ac_hash = to_bigint_or_none(row.get('ac_client_hash'))  # bigint
                erib_id = (row.get('erib_id') or None)

                oper_amt = to_decimal_or_none(row.get('oper_rur_amt'))  # numeric

                login_type = (row.get('login_type') or None)
                oper_type  = (row.get('oper_type') or None)

                dto = as_dt_or_none(row.get('date_time_oper'))     # timestamp
                dtc = as_dt_or_none(row.get('date_time_create'))   # timestamp
                if (row.get('date_time_oper') not in (None, '', 'NaT') and dto is None) \
                or (row.get('date_time_create') not in (None, '', 'NaT') and dtc is None):
                    bad_dt += 1
                    continue

                d_create = as_date_or_none(row.get('date_create'))  # date

                doc_type  = (row.get('doc_type') or None)
                recv_hash = to_bigint_or_none(row.get('receiver_client_hash'))  # bigint

                p2p = to_smallint_or_none(row.get('t_p2p_flg'))  # smallint

                rows.append((
                    ac_hash, erib_id, oper_amt, login_type, oper_type,
                    dto, d_create, dtc, doc_type, recv_hash, p2p
                ))

            if rows:
                try:
                    with connection.cursor() as cur:
                        cur.executemany(
                            'INSERT INTO so ("ac.client_hash", erib_id, oper_rur_amt, login_type, oper_type, '
                            'date_time_oper, date_create, date_time_create, doc_type, receiver_client_hash, "t.p2p_flg") '
                            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                            rows
                        )
                    msg = f"So: добавлено {len(rows)} (RAW SQL)"
                    if bad_dt:
                        msg += f", пропущено {bad_dt} строк из-за некорректных дат"
                    messages.append(msg)
                except Exception as e:
                    messages.append(f"So: ошибка вставки (RAW SQL): {e}")
            else:
                messages.append("So: нет валидных строк для вставки")



    # ---- Dog (RAW SQL: устойчиво к форматам) ----
    if files['dog']:
        df = read_excel(files['dog'], dtype={
            'ac_client_hash': str,
            'overdue_bucket_id': 'Int64',
            'overdue_bucket_name': str,
            'npl_nflag': str
        })
        if df is None:
            messages.append("Dog: файл не прочитан (пропущено)")
        else:
            df = df.dropna(how='all').copy()
            df.columns = [str(c).strip().replace('.', '_') for c in df.columns]

            # Преобразуем day_part к date
            if 'day_part' in df.columns:
                df['day_part'] = df['day_part'].apply(as_date_or_none)

            # Булевы как 0/1
            def to_int_bool_or_none(v):
                import pandas as pd
                if pd.isna(v):
                    return None
                if isinstance(v, str):
                    s = v.strip().lower()
                    if s in ('1','true','t','yes','y'): return 1
                    if s in ('0','false','f','no','n',''): return 0
                try:
                    return 1 if int(v) != 0 else 0
                except Exception:
                    # для любых truthy/falsey значений
                    return 1 if v else 0


            rows = []
            for _, row in df.iterrows():
                # ac_client_hash — BigInteger в БД
                ach = row.get('ac_client_hash')
                try:
                    ach_int = int(str(ach)) if ach is not None and str(ach).strip() != '' and not pd.isna(ach) else None
                except Exception:
                    ach_int = None

                # Числовые/дексимальные поля: NaN -> None (драйвер сам приведёт)
                def num(v):
                    return None if (isinstance(v, float) and pd.isna(v)) else v

                rows.append((
                    ach_int,
                    num(row.get('debt_due_bal_ccy_amt')),
                    num(row.get('debt_due_bal_rub_amt')),
                    num(row.get('debt_overdue_bal_ccy_amt')),
                    num(row.get('debt_overdue_bal_rub_amt')),
                    num(row.get('debt_intr_overdue_bal_ccy_amt')),
                    num(row.get('debt_intr_overdue_bal_rub_amt')),
                    num(row.get('debt_tot_os_ccy_amt')),
                    num(row.get('debt_tot_os_rub_amt')),
                    num(row.get('overdue_duration_days')),
                    num(row.get('debt_os_max_rub_amt')),
                    num(row.get('debt_ovrd_max_rub_amt')),
                    num(row.get('ovrd_max_dur_days')),
                    num(row.get('ovrd_tot_ever_days')),
                    num(row.get('ovrd_tot_entr_ever_qty')),
                    num(row.get('ovrd_max_rub_amt')),
                    num(row.get('total_overdue_duration_days')),
                    num(row.get('ovrd_tot_period_qty')),
                    num(row.get('ovrd_intr_bal_max_rub_amt')),
                    num(row.get('ovrd_intr_nobal_max_rub_amt')),
                    num(row.get('total_overdue_intr_bal_duration_days')),
                    num(row.get('total_overdue_intr_nobal_duration_days')),
                    num(row.get('overdue_bucket_id')),
                    (row.get('overdue_bucket_name') or None),
                    to_int_bool_or_none(row.get('npl_nflag')),
                    row.get('day_part')  # уже date или None
                ))

            if rows:
                try:
                    with connection.cursor() as cur:
                        cur.executemany(
                            'INSERT INTO core_dog ('
                            'ac_client_hash, '
                            'debt_due_bal_ccy_amt, debt_due_bal_rub_amt, '
                            'debt_overdue_bal_ccy_amt, debt_overdue_bal_rub_amt, '
                            'debt_intr_overdue_bal_ccy_amt, debt_intr_overdue_bal_rub_amt, '
                            'debt_tot_os_ccy_amt, debt_tot_os_rub_amt, '
                            'overdue_duration_days, '
                            'debt_os_max_rub_amt, debt_ovrd_max_rub_amt, '
                            'ovrd_max_dur_days, ovrd_tot_ever_days, ovrd_tot_entr_ever_qty, '
                            'ovrd_max_rub_amt, '
                            'total_overdue_duration_days, '
                            'ovrd_tot_period_qty, '
                            'ovrd_intr_bal_max_rub_amt, ovrd_intr_nobal_max_rub_amt, '
                            'total_overdue_intr_bal_duration_days, '
                            'total_overdue_intr_nobal_duration_days, '
                            'overdue_bucket_id, overdue_bucket_name, '
                            'npl_nflag, day_part'
                            ') VALUES ('
                            '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                            '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                            '%s,%s,%s,%s,%s,%s'
                            ')',
                            rows
                        )
                    messages.append(f"Dog: добавлено {len(rows)} (RAW SQL)")
                except Exception as e:
                    messages.append(f"Dog: ошибка вставки (RAW SQL): {e}")
            else:
                messages.append("Dog: нет строк для вставки")

    return render(request, 'core/upload_multi.html', {'messages': messages})

