import argparse
import os
import random
from datetime import datetime, timedelta, time, date
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
from faker import Faker

rnd = np.random.default_rng(42)
fake = Faker('ru_RU')

# Базовые пресеты по СПб (можно расширить)
CITY_PRESETS = {
    "spb": {
        "name": "Санкт-Петербург",
        # Примерные центроиды районов для сидов “дома/работы”
        "home_centers": [
            (59.9343, 30.3351),   # центр
            (59.8661, 30.3215),   # Московский
            (60.0089, 30.2588),   # Приморский
            (59.8510, 30.2686),   # Купчино
            (60.0200, 30.3890),   # Парнас
            (59.9802, 30.3434),   # Петроградка
        ],
        "work_centers": [
            (59.9311, 30.3609),   # Невский/офисы
            (59.9087, 30.4826),   # технопарк
            (60.0045, 30.3000),   # IT‑офисы
            (59.8487, 30.2945),   # юг БЦ
        ],
        # Частые мерчанты (название, mcc, тип)
        "merchants": [
            ("ПЯТЁРОЧКА", 5411, "grocery"),
            ("ВКУСВИЛЛ", 5499, "grocery"),
            ("МАГНИТ", 5411, "grocery"),
            ("КФС", 5814, "food"),
            ("ДОДО ПИЦЦА", 5814, "food"),
            ("STARBUCKS", 5814, "coffee"),
            ("ШОКОЛАДНИЦА", 5814, "coffee"),
            ("АТБ", 5411, "grocery"),
            ("АЗС G-DRIVE", 5541, "fuel"),
            ("METRO", 5411, "grocery"),
            ("OZON", 5969, "ecom"),
            ("WB", 5969, "ecom"),
            ("SBOL", 6012, "p2p"),
        ],
        "cities": ["Санкт-Петербург", "Пушкин", "Павловск", "Колпино", "Сестрорецк"],
    }
}

def jitter_coord(lat, lon, max_m=300):
    # случайный сдвиг в радиусе max_m метров
    # грубая аппроксимация 1e-5 ~ 1.11 м по широте
    d = max_m * rnd.random()
    ang = rnd.random() * 2 * np.pi
    dy = (d * np.cos(ang)) / 111_000.0
    dx = (d * np.sin(ang)) / (111_000.0 * np.cos(np.deg2rad(lat)))
    return lat + dy, lon + dx

def pick_center(centers):
    return random.choice(centers)

def iso_dt(d: datetime) -> str:
    # без TZ
    return d.strftime("%Y-%m-%dT%H:%M:%S")

def day_part(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")

def rand_time_in_window(base_date: date, start_h: int, end_h: int) -> datetime:
    h = rnd.integers(start_h, end_h)
    m = rnd.integers(0, 60)
    s = rnd.integers(0, 60)
    return datetime.combine(base_date, time(int(h), int(m), int(s)))

def gen_home_work_for_client(city_cfg):
    home_lat, home_lon = jitter_coord(*pick_center(city_cfg["home_centers"]), max_m=500)
    work_lat, work_lon = jitter_coord(*pick_center(city_cfg["work_centers"]), max_m=400)
    return (home_lat, home_lon), (work_lat, work_lon)

def near(lat, lon, scatter_m=150):
    return jitter_coord(lat, lon, max_m=scatter_m)

def choose_merchant(city_cfg, context="generic"):
    M = city_cfg["merchants"]
    if context == "morning":
        weights = [0.05,0.05,0.02,0.10,0.10,0.25,0.20,0.01,0.00,0.02,0.05,0.05,0.10]
    elif context == "lunch":
        weights = [0.03,0.03,0.02,0.25,0.25,0.10,0.15,0.01,0.00,0.01,0.07,0.07,0.02]
    elif context == "evening":
        weights = [0.20,0.15,0.10,0.10,0.10,0.10,0.05,0.10,0.05,0.05,0.00,0.00,0.00]
    else:
        weights = None
    if weights:
        idx = rnd.choice(len(M), p=np.array(weights)/sum(weights))
        return M[int(idx)]
    return random.choice(M)

def rand_amount(kind: str) -> float:
    if kind == "coffee":
        return round(float(rnd.normal(250, 80)), 2)
    if kind == "food":
        return round(float(rnd.normal(700, 250)), 2)
    if kind == "grocery":
        return round(float(abs(rnd.normal(1200, 600))), 2)
    if kind == "fuel":
        return round(float(abs(rnd.normal(2500, 800))), 2)
    if kind == "ecom":
        return round(float(abs(rnd.normal(2000, 1200))), 2)
    if kind == "p2p":
        return round(float(abs(rnd.normal(3000, 1500))), 2)
    return round(float(abs(rnd.normal(1000, 700))), 2)

def gen_clients(n: int, start_date: date, days: int, city="spb", p_debtor=0.2):
    city_cfg = CITY_PRESETS[city]
    clients = []
    for i in range(n):
        # client_hash — большие целые, как в DOG (BigInt)
        ch = int(rnd.integers(10**17, 10**18-1))
        home, work = gen_home_work_for_client(city_cfg)
        is_debtor = rnd.random() < p_debtor
        clients.append({
            "hash": ch,
            "home": home,
            "work": work,
            "is_debtor": is_debtor,
        })
    cs_rows, c_rows, tr_rows, so_rows, dog_rows = [], [], [], [], []
    for cl in clients:
        ch = cl["hash"]
        (h_lat, h_lon), (w_lat, w_lon) = cl["home"], cl["work"]
        is_debtor = cl["is_debtor"]

        for d in range(days):
            day = start_date + timedelta(days=d)
            weekday = day.weekday()  # 0=Mon

            # ——— CS: гео-события, чтобы кластеризовалось
            # Ночь/утро у дома
            for _ in range(rnd.integers(2,5)):
                t = rand_time_in_window(day, 6, 9)
                lat, lon = near(h_lat, h_lon, scatter_m=120)
                cs_rows.append({
                    "ac.client_hash": str(ch),
                    "eventaction": "app_open",
                    "geolatitude": lat, "geolongitude": lon,
                    "dt": t, "date_part": day_part(t)
                })
            # День — работа (будни)
            if weekday < 5:
                for _ in range(rnd.integers(3,6)):
                    t = rand_time_in_window(day, 11, 16)
                    lat, lon = near(w_lat, w_lon, scatter_m=120)
                    cs_rows.append({
                        "ac.client_hash": str(ch),
                        "eventaction": "view",
                        "geolatitude": lat, "geolongitude": lon,
                        "dt": t, "date_part": day_part(t)
                    })
            # Вечер — дом
            for _ in range(rnd.integers(2,5)):
                t = rand_time_in_window(day, 19, 23)
                lat, lon = near(h_lat, h_lon, scatter_m=120)
                cs_rows.append({
                    "ac.client_hash": str(ch),
                    "eventaction": "app_open",
                    "geolatitude": lat, "geolongitude": lon,
                    "dt": t, "date_part": day_part(t)
                })

            # ——— TR & C: транзакции
            # Утренний кофе/транспорт у дома
            if rnd.random() < 0.7:
                t = rand_time_in_window(day, 7, 10)
                m = choose_merchant(city_cfg, "morning")
                amt = rand_amount(m[2])
                tr_rows.append({
                    "src": "core",
                    "ac.client_hash": str(ch),
                    "c_txn_dt": t,
                    "t_trx_city": random.choice(city_cfg["cities"]),
                    "t_mcc_code": m[1],
                    "t_trans_type": None,
                    "t_trx_direction": "D",
                    "t_merchant_id": None,
                    "t_terminal_id": None,
                    "t_merchant_name": m,
                    "c_txn_rub_amt": amt,
                    "day_part": day_part(t),
                })
                # слой C (агрегат) — дублируем часть записей
                if rnd.random() < 0.8:
                    c_rows.append({
                        "src": "core",
                        "ac.client_hash": str(ch),
                        "c_txn_dt": t,
                        "txn_cod_type_rk": m[1],
                        "txn_cod_type_name": m,
                        "c_txn_rub_amt": amt,
                        "pmnt_payer_name": m,
                        "day_part": day_part(t),
                    })

            # Обед рядом с работой (будни)
            if weekday < 5 and rnd.random() < 0.8:
                t = rand_time_in_window(day, 12, 15)
                m = choose_merchant(city_cfg, "lunch")
                amt = rand_amount(m[2])
                tr_rows.append({
                    "src": "core",
                    "ac.client_hash": str(ch),
                    "c_txn_dt": t,
                    "t_trx_city": random.choice(city_cfg["cities"]),
                    "t_mcc_code": m[1],
                    "t_trans_type": None,
                    "t_trx_direction": "D",
                    "t_merchant_id": None,
                    "t_terminal_id": None,
                    "t_merchant_name": m,
                    "c_txn_rub_amt": amt,
                    "day_part": day_part(t),
                })
                if rnd.random() < 0.8:
                    c_rows.append({
                        "src": "core",
                        "ac.client_hash": str(ch),
                        "c_txn_dt": t,
                        "txn_cod_type_rk": m[1],
                        "txn_cod_type_name": m,
                        "c_txn_rub_amt": amt,
                        "pmnt_payer_name": m,
                        "day_part": day_part(t),
                    })

            # Вечерние закупки у дома
            if rnd.random() < 0.9:
                t = rand_time_in_window(day, 18, 22)
                m = choose_merchant(city_cfg, "evening")
                amt = rand_amount(m[2])
                tr_rows.append({
                    "src": "core",
                    "ac.client_hash": str(ch),
                    "c_txn_dt": t,
                    "t_trx_city": random.choice(city_cfg["cities"]),
                    "t_mcc_code": m[1],
                    "t_trans_type": None,
                    "t_trx_direction": "D",
                    "t_merchant_id": None,
                    "t_terminal_id": None,
                    "t_merchant_name": m,
                    "c_txn_rub_amt": amt,
                    "day_part": day_part(t),
                })
                if rnd.random() < 0.8:
                    c_rows.append({
                        "src": "core",
                        "ac_client_hash": str(ch),
                        "ac.client_hash": str(ch),  # подстраховка для разных шаблонов
                        "c_txn_dt": t,
                        "txn_cod_type_rk": m[1],
                        "txn_cod_type_name": m,
                        "c_txn_rub_amt": amt,
                        "pmnt_payer_name": m,
                        "day_part": day_part(t),
                    })

            # P2P и SO в рабочие часы и вечером
            if rnd.random() < 0.4:
                t = rand_time_in_window(day, 10, 20)
                so_rows.append({
                    "ac.client_hash": str(ch),
                    "erib_id": str(rnd.integers(10**8, 10**9-1)),
                    "oper_rur_amt": round(float(abs(rnd.normal(1500, 900))),2),
                    "login_type": "mobile" if rnd.random()<0.7 else "web",
                    "oper_type": "p2p",
                    "date_time_oper": t,
                    "date_create": day_part(t),
                    "date_time_create": t + timedelta(minutes=int(rnd.integers(1, 30))),
                    "doc_type": "P2P",
                    "receiver_client_hash": str(rnd.integers(10**9, 10**10-1)),
                    "t.p2p_flg": "1",
                    "day_part": day_part(t),
                })

        # DOG для должников (агрегированные поля в пределах разумного)
        if is_debtor:
            today = start_date + timedelta(days=days-1)
            bucket_id = random.choice([1,3,5,7])
            dog_rows.append({
                "ac_client_hash": str(ch),
                "debt_due_bal_ccy_amt": round(float(abs(rnd.normal(20000, 12000))),2),
                "debt_due_bal_rub_amt": round(float(abs(rnd.normal(20000, 12000))),2),
                "debt_overdue_bal_ccy_amt": round(float(abs(rnd.normal(5000, 4000))),2),
                "debt_overdue_bal_rub_amt": round(float(abs(rnd.normal(5000, 4000))),2),
                "debt_intr_overdue_bal_ccy_amt": round(float(abs(rnd.normal(1500, 1200))),2),
                "debt_intr_overdue_bal_rub_amt": round(float(abs(rnd.normal(1500, 1200))),2),
                "debt_tot_os_ccy_amt": round(float(abs(rnd.normal(25000, 15000))),2),
                "debt_tot_os_rub_amt": round(float(abs(rnd.normal(25000, 15000))),2),
                "overdue_duration_days": int(abs(rnd.normal(25, 10))),
                "debt_os_max_rub_amt": round(float(abs(rnd.normal(35000, 20000))),2),
                "debt_ovrd_max_rub_amt": round(float(abs(rnd.normal(12000, 8000))),2),
                "ovrd_max_dur_days": int(abs(rnd.normal(40, 15))),
                "ovrd_tot_ever_days": int(abs(rnd.normal(120, 60))),
                "ovrd_tot_entr_ever_qty": int(abs(rnd.normal(5, 3))),
                "ovrd_max_rub_amt": round(float(abs(rnd.normal(15000, 9000))),2),
                "total_overdue_duration_days": int(abs(rnd.normal(200, 80))),
                "ovrd_tot_period_qty": int(abs(rnd.normal(8, 4))),
                "ovrd_intr_bal_max_rub_amt": round(float(abs(rnd.normal(4000, 2000))),2),
                "ovrd_intr_nobal_max_rub_amt": round(float(abs(rnd.normal(4000, 2000))),2),
                "total_overdue_intr_bal_duration_days": int(abs(rnd.normal(60, 25))),
                "total_overdue_intr_nobal_duration_days": int(abs(rnd.normal(40, 20))),
                "overdue_bucket_id": bucket_id,
                "overdue_bucket_name": f"{bucket_id*30-30}+",
                "npl_nflag": "1",
                "day_part": today.strftime("%Y-%m-%d"),
            })

    # Формируем датафреймы по шаблонам
    cs = pd.DataFrame(cs_rows, columns=["ac.client_hash","eventaction","geolatitude","geolongitude","dt","date_part"])
    c  = pd.DataFrame(c_rows, columns=["src","ac.client_hash","c_txn_dt","txn_cod_type_rk","txn_cod_type_name","c_txn_rub_amt","pmnt_payer_name","day_part"])
    tr = pd.DataFrame(tr_rows, columns=["src","ac.client_hash","c_txn_dt","t_trx_city","t_mcc_code","t_trans_type","t_trx_direction","t_merchant_id","t_terminal_id","t_merchant_name","c_txn_rub_amt","day_part"])
    so = pd.DataFrame(so_rows, columns=["ac.client_hash","erib_id","oper_rur_amt","login_type","oper_type","date_time_oper","date_create","date_time_create","doc_type","receiver_client_hash","t.p2p_flg","day_part"])
    dog= pd.DataFrame(dog_rows, columns=["ac_client_hash","debt_due_bal_ccy_amt","debt_due_bal_rub_amt","debt_overdue_bal_ccy_amt","debt_overdue_bal_rub_amt","debt_intr_overdue_bal_ccy_amt","debt_intr_overdue_bal_rub_amt","debt_tot_os_ccy_amt","debt_tot_os_rub_amt","overdue_duration_days","debt_os_max_rub_amt","debt_ovrd_max_rub_amt","ovrd_max_dur_days","ovrd_tot_ever_days","ovrd_tot_entr_ever_qty","ovrd_max_rub_amt","total_overdue_duration_days","ovrd_tot_period_qty","ovrd_intr_bal_max_rub_amt","ovrd_intr_nobal_max_rub_amt","total_overdue_intr_bal_duration_days","total_overdue_intr_nobal_duration_days","overdue_bucket_id","overdue_bucket_name","npl_nflag","day_part"])

    return cs, c, tr, so, dog

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clients", type=int, default=30)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--out", type=str, default="./fake_data")
    ap.add_argument("--city", type=str, default="spb")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    start_date = (datetime.utcnow().date() - relativedelta(days=args.days))

    cs, c, tr, so, dog = gen_clients(args.clients, start_date, args.days, city=args.city)

    # Сохраняем в 5 Excel‑файлов (по одному листу)
    cs.to_excel(os.path.join(args.out, "template_cs.xlsx"), index=False)
    c.to_excel(os.path.join(args.out, "template_c.xlsx"), index=False)
    tr.to_excel(os.path.join(args.out, "template_tr.xlsx"), index=False)
    so.to_excel(os.path.join(args.out, "template_so.xlsx"), index=False)
    dog.to_excel(os.path.join(args.out, "template_dog.xlsx"), index=False)

    print(f"Saved to {args.out}")

if __name__ == "__main__":
    main()
