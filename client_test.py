import os
from datetime import datetime, timedelta
import pandas as pd

OUT_DIR = "./one_client"
CLIENT = "1112815496341993984"  # BigInt как строка в Excel
HOME = (59.8510, 30.2686)       # Купчино
WORK = (59.8661, 30.3215)       # Московские Ворота

# Вспомогательные
def iso_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

def day_part(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

# Базовые даты (3 последовательных будних дня)
d0 = datetime(2025, 8, 18)  # Пн
days = [d0, d0 + timedelta(days=1), d0 + timedelta(days=2)]

# 1) CS: гео-события (утро/вечер у дома, день — у работы)
cs_rows = []
for base in days:
    # утро у дома
    for h in (7, 8):
        dt = base.replace(hour=h, minute=15, second=0)
        cs_rows.append({
            "ac.client_hash": CLIENT,
            "eventaction": "app_open",
            "geolatitude": HOME[0] + 0.0007,
            "geolongitude": HOME[1] + 0.0007,
            "dt": iso_dt(dt),
            "date_part": day_part(dt),
        })
    # день у работы
    for h in (12, 14, 16):
        dt = base.replace(hour=h, minute=20, second=0)
        cs_rows.append({
            "ac.client_hash": CLIENT,
            "eventaction": "view",
            "geolatitude": WORK[0] - 0.0008,
            "geolongitude": WORK[1] + 0.0004,
            "dt": iso_dt(dt),
            "date_part": day_part(dt),
        })
    # вечер у дома
    for h in (19, 21):
        dt = base.replace(hour=h, minute=35, second=0)
        cs_rows.append({
            "ac.client_hash": CLIENT,
            "eventaction": "app_open",
            "geolatitude": HOME[0] - 0.0006,
            "geolongitude": HOME[1] - 0.0005,
            "dt": iso_dt(dt),
            "date_part": day_part(dt),
        })

cs = pd.DataFrame(cs_rows, columns=[
    "ac.client_hash","eventaction","geolatitude","geolongitude","dt","date_part"
])

# 2) TR: транзакции (утро — кофе у дома, обед — у работы, вечер — покупки у дома)
tr_rows = []
LUNCH  = ("ШОКОЛАДНИЦА", 5814, "Санкт-Петербург")
COFFEE = ("STARBUCKS",    5814, "Санкт-Петербург")
GROC   = ("ПЯТЁРОЧКА",    5411, "Санкт-Петербург")

for base in days:
    # утренний кофе у дома
    t1 = base.replace(hour=8, minute=10, second=0)
    tr_rows.append({
        "src": "core",
        "ac.client_hash": CLIENT,
        "c_txn_dt": iso_dt(t1),
        "t_trx_city": COFFEE[2],
        "t_mcc_code": COFFEE[1],
        "t_trans_type": "",
        "t_trx_direction": "D",
        "t_merchant_id": "",
        "t_terminal_id": "",
        "t_merchant_name": COFFEE[0],  # строка, не кортеж!
        "c_txn_rub_amt": 230.00,
        "day_part": day_part(t1),
    })
    # обед у работы
    t2 = base.replace(hour=13, minute=5, second=0)
    tr_rows.append({
        "src": "core",
        "ac.client_hash": CLIENT,
        "c_txn_dt": iso_dt(t2),
        "t_trx_city": LUNCH[2],
        "t_mcc_code": LUNCH[1],
        "t_trans_type": "",
        "t_trx_direction": "D",
        "t_merchant_id": "",
        "t_terminal_id": "",
        "t_merchant_name": LUNCH[0],
        "c_txn_rub_amt": 690.00,
        "day_part": day_part(t2),
    })
    # вечерние покупки у дома
    t3 = base.replace(hour=20, minute=15, second=0)
    tr_rows.append({
        "src": "core",
        "ac.client_hash": CLIENT,
        "c_txn_dt": iso_dt(t3),
        "t_trx_city": GROC[2],
        "t_mcc_code": GROC[1],
        "t_trans_type": "",
        "t_trx_direction": "D",
        "t_merchant_id": "",
        "t_terminal_id": "",
        "t_merchant_name": GROC[0],
        "c_txn_rub_amt": 1450.00,
        "day_part": day_part(t3),
    })

tr = pd.DataFrame(tr_rows, columns=[
    "src","ac.client_hash","c_txn_dt","t_trx_city","t_mcc_code","t_trans_type",
    "t_trx_direction","t_merchant_id","t_terminal_id","t_merchant_name","c_txn_rub_amt","day_part"
])

# 3) C: дубли основных операций для совместимости
c_rows = []
for base in days:
    # утренний кофе
    t1 = base.replace(hour=8, minute=10, second=0)
    c_rows.append({
        "src": "core",
        "ac.client_hash": CLIENT,
        "c_txn_dt": iso_dt(t1),
        "txn_cod_type_rk": 5814,
        "txn_cod_type_name": "STARBUCKS",
        "c_txn_rub_amt": 230.00,
        "pmnt_payer_name": "STARBUCKS",
        "day_part": day_part(t1),
    })
    # обед
    t2 = base.replace(hour=13, minute=5, second=0)
    c_rows.append({
        "src": "core",
        "ac_client_hash": CLIENT,          # на случай чтения как ac_client_hash
        "ac.client_hash": CLIENT,          # и на случай ac.client_hash
        "c_txn_dt": iso_dt(t2),
        "txn_cod_type_rk": 5814,
        "txn_cod_type_name": "ШОКОЛАДНИЦА",
        "c_txn_rub_amt": 690.00,
        "pmnt_payer_name": "ШОКОЛАДНИЦА",
        "day_part": day_part(t2),
    })
    # вечер
    t3 = base.replace(hour=20, minute=15, second=0)
    c_rows.append({
        "src": "core",
        "ac.client_hash": CLIENT,
        "c_txn_dt": iso_dt(t3),
        "txn_cod_type_rk": 5411,
        "txn_cod_type_name": "ПЯТЁРОЧКА",
        "c_txn_rub_amt": 1450.00,
        "pmnt_payer_name": "ПЯТЁРОЧКА",
        "day_part": day_part(t3),
    })

# В колонках C допускаем и ac.client_hash, и ac_client_hash — твой загрузчик их нормализует
c = pd.DataFrame(c_rows, columns=[
    "src","ac.client_hash","ac_client_hash","c_txn_dt","txn_cod_type_rk",
    "txn_cod_type_name","c_txn_rub_amt","pmnt_payer_name","day_part"
])

# 4) SO: одна P2P-операция в каждый день (рабочие часы)
so_rows = []
for base in days:
    t = base.replace(hour=15, minute=20, second=0)
    so_rows.append({
        "ac.client_hash": CLIENT,
        "erib_id": "123456789",
        "oper_rur_amt": 2500.00,
        "login_type": "mobile",
        "oper_type": "p2p",
        "date_time_oper": iso_dt(t),
        "date_create": day_part(t),
        "date_time_create": iso_dt(t + timedelta(minutes=5)),
        "doc_type": "P2P",
        "receiver_client_hash": "9990001",
        "t.p2p_flg": "1",
        "day_part": day_part(t),
    })

so = pd.DataFrame(so_rows, columns=[
    "ac.client_hash","erib_id","oper_rur_amt","login_type","oper_type",
    "date_time_oper","date_create","date_time_create","doc_type",
    "receiver_client_hash","t.p2p_flg","day_part"
])

# 5) DOG: пустой (без долгов)
dog = pd.DataFrame([], columns=[
    "ac_client_hash","debt_due_bal_ccy_amt","debt_due_bal_rub_amt",
    "debt_overdue_bal_ccy_amt","debt_overdue_bal_rub_amt",
    "debt_intr_overdue_bal_ccy_amt","debt_intr_overdue_bal_rub_amt",
    "debt_tot_os_ccy_amt","debt_tot_os_rub_amt","overdue_duration_days",
    "debt_os_max_rub_amt","debt_ovrd_max_rub_amt","ovrd_max_dur_days",
    "ovrd_tot_ever_days","ovrd_tot_entr_ever_qty","ovrd_max_rub_amt",
    "total_overdue_duration_days","ovrd_tot_period_qty",
    "ovrd_intr_bal_max_rub_amt","ovrd_intr_nobal_max_rub_amt",
    "total_overdue_intr_bal_duration_days","total_overdue_intr_nobal_duration_days",
    "overdue_bucket_id","overdue_bucket_name","npl_nflag","day_part"
])

# Сохранение
os.makedirs(OUT_DIR, exist_ok=True)
cs.to_excel(os.path.join(OUT_DIR, "template_cs.xlsx"), index=False)
# Для C сохраним безопасно только те колонки, что точно есть в шаблоне
c[[ "src","ac.client_hash","c_txn_dt","txn_cod_type_rk","txn_cod_type_name",
    "c_txn_rub_amt","pmnt_payer_name","day_part" ]].to_excel(os.path.join(OUT_DIR, "template_c.xlsx"), index=False)
tr.to_excel(os.path.join(OUT_DIR, "template_tr.xlsx"), index=False)
so.to_excel(os.path.join(OUT_DIR, "template_so.xlsx"), index=False)
dog.to_excel(os.path.join(OUT_DIR, "template_dog.xlsx"), index=False)

print(f"Saved to {OUT_DIR}")
