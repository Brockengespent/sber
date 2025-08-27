import random
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db import transaction, connection
from django.db.models import Q

from core.models import Dog, Tr, So  # Cs и C пишем сырым SQL; clients_city (view) не трогаем

random.seed(7)

# MCC и мерчанты
MCC = {
    'grocery':  [5411, 5499],
    'coffee':   [5813],
    'food':     [5814, 5812],
    'ecom':     [5969],
    'fuel':     [5541, 5542],
    'pharmacy': [5912],
    'transport':[4111, 4121],
    'atm':      [6011],
}
MERCH = {
    'grocery':  ['ПЯТЁРОЧКА','МАГНИТ','ВКУСВИЛЛ'],
    'coffee':   ['STARBUCKS','КОФЕ ХАУЗ'],
    'food':     ['ДОДО ПИЦЦА','KFC','SUBWAY'],
    'ecom':     ['OZON','WB','ALIEXPRESS'],
    'fuel':     ['GAZPROMNEFT','LUKOIL','SHELL'],
    'pharmacy': ['36.6','АПТЕКА'],
    'transport':['YANDEX TAXI','UBER'],
    'atm':      ['ATM SBER'],
}
CITIES = ['Санкт-Петербург','Павловск','Колпино','Пушкин','Сестрорецк','—']

def rub(v): return round(float(v), 2)
def rnd(a): return random.choice(a)
def dt_days_ago(n): return timezone.now() - timedelta(days=n)

# Профили клиентов
PROFILES = [
    dict(  # Клиент A — офисный, без долга
        client_hash=922337203685477111, city='Санкт-Петербург',
        debt=0, bucket='0', npl=False, salary_day=25,
        home=(59.9343, 30.3351), work=(59.9450, 30.3200),
        mix=[('grocery',0.35),('coffee',0.20),('food',0.15),('ecom',0.25),('pharmacy',0.05)],
    ),
    dict(  # Клиент B — таксист, малая просрочка
        client_hash=922337203685477112, city='Колпино',
        debt=45500, bucket='1-30', npl=False, salary_day=10,
        home=(59.7500, 30.6000), work=(59.8000, 30.6500),
        mix=[('fuel',0.45),('food',0.15),('coffee',0.10),('grocery',0.20),('transport',0.10)],
    ),
    dict(  # Клиент C — онлайн‑шоппер, 60–90, NPL
        client_hash=922337203685477113, city='Павловск',
        debt=320000, bucket='60-90', npl=True, salary_day=1,
        home=(59.6833, 30.4500), work=(59.7200, 30.4800),
        mix=[('ecom',0.50),('grocery',0.20),('food',0.10),('pharmacy',0.10),('coffee',0.05),('atm',0.05)],
    ),
]

class Command(BaseCommand):
    help = "Эталонные клиенты (3 профиля) с зарплатой, MCC, гео и связками So↔C"

    @transaction.atomic
    def handle(self, *args, **opts):
        # --- подготовка массивов id ---
        hs_int = [int(p['client_hash']) for p in PROFILES]
        hs_str = [str(p['client_hash']) for p in PROFILES]

        # --- очистка данных для этих клиентов ---
        with connection.cursor() as cur:
            # cs и c: bigint-колонки → приводим параметр к bigint[]
            cur.execute('DELETE FROM cs WHERE "ac.client_hash" = ANY(%s::bigint[])', (hs_int,))
            cur.execute('DELETE FROM c  WHERE "ac.client_hash" = ANY(%s::bigint[])', (hs_int,))
        # so и tr: чистим через ORM
        So.objects.filter(Q(ac_client_hash__in=hs_str) | Q(ac_client_hash__in=hs_int)).delete()
        Tr.objects.filter(Q(ac_client_hash__in=hs_str) | Q(ac_client_hash__in=hs_int)).delete()
        Dog.objects.filter(ac_client_hash__in=hs_int).delete()

        # --- core_dog: INSERT если нет + UPDATE (npl_nflag = 0/1) ---
        with connection.cursor() as cur:
            for p in PROFILES:
                npl_val = 1 if p['npl'] else 0
                cur.execute("""
                    INSERT INTO core_dog (ac_client_hash, debt_tot_os_rub_amt, overdue_bucket_name, npl_nflag, day_part)
                    SELECT %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (SELECT 1 FROM core_dog WHERE ac_client_hash = %s)
                """, [p['client_hash'], rub(p['debt']), p['bucket'], npl_val, timezone.now().date(), p['client_hash']])
                cur.execute("""
                    UPDATE core_dog
                       SET debt_tot_os_rub_amt = %s,
                           overdue_bucket_name = %s,
                           npl_nflag = %s,
                           day_part = %s
                     WHERE ac_client_hash = %s
                """, [rub(p['debt']), p['bucket'], npl_val, timezone.now().date(), p['client_hash']])

        # --- helpers для сырых вставок в cs и c ---
        def insert_cs(ac_hash: str, lat: float, lon: float, dt_val):
            with connection.cursor() as cur:
                cur.execute("""
                    INSERT INTO cs ("ac.client_hash", eventaction, geolatitude, geolongitude, dt, date_part)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, [ac_hash, 'Login Success', lat, lon, dt_val, dt_val.date()])

        def insert_c(ac_hash: str, dt_val, txn_rk: int, txn_name: str, amt, payer: str):
            with connection.cursor() as cur:
                cur.execute("""
                    INSERT INTO c (src, "ac.client_hash", c_txn_dt, txn_cod_type_rk, txn_cod_type_name,
                                   c_txn_rub_amt, pmnt_payer_name, day_part)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, ['demo', ac_hash, dt_val, txn_rk, txn_name, amt, payer, dt_val.date()])

        # --- гео для теплокарты (cs) ---
        for p in PROFILES:
            h = str(p['client_hash'])
            (hlat, hlon) = p['home']; (wlat, wlon) = p['work']
            for d in range(1, 91):
                # дом утром
                lat = hlat + random.uniform(-0.003,0.003)
                lon = hlon + random.uniform(-0.003,0.003)
                dt_val = dt_days_ago(d).replace(hour=8, minute=random.randint(0,40))
                insert_cs(h, lat, lon, dt_val)
                # работа днём (будни)
                if dt_days_ago(d).weekday() < 5:
                    lat = wlat + random.uniform(-0.003,0.003)
                    lon = wlon + random.uniform(-0.003,0.003)
                    dt_val = dt_days_ago(d).replace(hour=14, minute=random.randint(0,40))
                    insert_cs(h, lat, lon, dt_val)
                # визиты по выходным
                if dt_days_ago(d).weekday() >= 5 and random.random() < 0.3:
                    lat = rnd([59.93,59.74,59.80]) + random.uniform(-0.01,0.01)
                    lon = rnd([30.31,30.60,30.50]) + random.uniform(-0.01,0.01)
                    dt_val = dt_days_ago(d).replace(hour=19, minute=random.randint(0,40))
                    insert_cs(h, lat, lon, dt_val)

        # --- поступления: зарплата (3 месяца) + p2p ---
        for p in PROFILES:
            h = str(p['client_hash'])
            base = timezone.now().date().replace(day=1)
            months = [base, (base - timedelta(days=30)), (base - timedelta(days=60))]
            for m in months:
                day = min(p['salary_day'], 28)
                dts = timezone.make_aware(timezone.datetime(m.year, m.month, day, 12, 0, 0))
                amt = rub(random.randint(80_000, 180_000))
                insert_c(h, dts, 100, 'ЗАРПЛАТА', amt, 'ЗАРПЛАТА')
                def insert_so(ac_hash: str, erib_id: str, amt, dts):
                    with connection.cursor() as cur:
                        cur.execute("""
                            INSERT INTO so ("ac.client_hash", erib_id, oper_rur_amt, login_type, oper_type,
                                            date_time_oper, date_create, date_time_create, doc_type,
                                            receiver_client_hash, "t.p2p_flg", day_part)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, [ac_hash, erib_id, amt, 'web', 'credit', dts, dts.date(), dts, 'Зарплата', None, 0, dts.date()])
            # p2p
            for _ in range(3):
                d = random.randint(5, 80)
                dts = dt_days_ago(d)
                insert_c(h, dts, 200, 'Перевод', rub(random.randint(500, 8000)),
                         rnd(['P2P ПОЛУЧЕНИЕ','ПЕРЕВОД ОТ ДРУГА']))

        # --- карточные операции Tr (расходы + возвраты + ATM) ---
        for p in PROFILES:
            h = str(p['client_hash'])
            cats, weights = zip(*p['mix'])
            # ежедневные расходы
            for d in range(1, 91):
                cat = random.choices(cats, weights=weights, k=1)[0]
                mcc = rnd(MCC[cat]); name = rnd(MERCH[cat])
                for _ in range(random.choice([0,1,1,2])):
                    amt = rub(random.randint(200, 6000))
                    Tr.objects.create(
                        src='demo', ac_client_hash=h, c_txn_dt=dt_days_ago(d),
                        t_trx_city=rnd(CITIES), txn_cod_type_rk=mcc,
                        t_trx_direction='D', t_merchant_name=name,
                        c_txn_rub_amt=amt, day_part=dt_days_ago(d).date()
                    )
            # возвраты
            for d in [7, 21, 45]:
                name = rnd(MERCH['food'])
                Tr.objects.create(
                    src='demo', ac_client_hash=h, c_txn_dt=dt_days_ago(d),
                    t_trx_city='—', txn_cod_type_rk=5812,
                    t_trx_direction='C', t_merchant_name=name,
                    c_txn_rub_amt=rub(random.randint(150, 1200)),
                    day_part=dt_days_ago(d).date()
                )
            # ATM — только для клиентов с долгом
            if p['debt'] != 0:
                for _ in range(2):
                    d = random.randint(10, 80)
                    Tr.objects.create(
                        src='demo', ac_client_hash=h, c_txn_dt=dt_days_ago(d),
                        t_trx_city=rnd(CITIES), txn_cod_type_rk=6011,
                        t_trx_direction='D', t_merchant_name='ATM SBER',
                        c_txn_rub_amt=rub(random.randint(1000, 8000)),
                        day_part=dt_days_ago(d).date()
                    )

        self.stdout.write(self.style.SUCCESS("Эталонные клиенты (3 профиля) сгенерированы."))
