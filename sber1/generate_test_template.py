import pandas as pd

# 1) cs
cs = pd.DataFrame([
    ["1111111111", "login", 59.9343, 30.3351, "2025-02-14 09:00:00", "2025-02-14"],
    ["2222222222", "purchase", 55.7558, 37.6173, "2025-02-15 15:30:00", "2025-02-15"],
], columns=[
    "ac.client_hash", "eventaction", "geolatitude", "geolongitude", "dt", "date_part"
])

# 2) c
c = pd.DataFrame([
    ["cod", "1111111111", "2025-02-13 00:00:00", 2, "Доп. взнос", 400.00, "TEST PAYER 1", "2025-02-13"],
    ["cod", "2222222222", "2025-02-14 00:00:00", 3, "Частичная выдача", 1500.00, "TEST PAYER 2", "2025-02-14"],
], columns=[
    "src", "ac.client_hash", "c_txn_dt", "txn_cod_type_rk", "txn_cod_type_name",
    "c_txn_rub_amt", "pmnt_payer_name", "day_part"
])

# 3) tr
tr = pd.DataFrame([
    ["txn", "1111111111", "2025-02-14 17:39:44", 5411, "Продукты", 150.00, "MAGAZIN 1", "2025-02-14"],
    ["txn", "2222222222", "2025-02-15 10:12:00", 5814, "Кафе",     500.00, "COFFEE POINT", "2025-02-15"],
], columns=[
    "src", "ac.client_hash", "c_txn_dt", "txn_cod_type_rk", "txn_cod_type_name",
    "c_txn_rub_amt", "pmnt_payer_name", "day_part"
])

# 4) so
so = pd.DataFrame([
    ["1111111111", "ERIB0001", 200.00, "MAPI", "int", "2025-02-14 14:00:00", "2025-02-14", "2025-02-14 14:05:00", "Transfer", "2222222222", 1, "2025-02-14"],
    ["2222222222", "ERIB0002", 1000.00, "MAPI", "int", "2025-02-15 09:30:00", "2025-02-15", "2025-02-15 09:35:00", "Payment", None, 0, "2025-02-15"],
], columns=[
    "ac.client_hash", "erib_id", "oper_rur_amt", "login_type", "oper_type",
    "date_time_oper", "date_create", "date_time_create", "doc_type",
    "receiver_client_hash", "t.p2p_flg", "day_part"
])

# 5) dog
dog = pd.DataFrame([
    ["1111111111", 0.00, 0.00, 5000.00, 5000.00, 200.00, 200.00, 5200.00, 5200.00, 180, 4000.00, 3000.00, 200, 400, 2, 3500.00, 365, 3, 1500.00, 1200.00, 90, 60, 6, "180+", 1, "2025-02-14"],
    ["2222222222", 1000.00, 1000.00, 0.00, 0.00, 0.00, 0.00, 1000.00, 1000.00, 0,  0.00,   0.00,   0,   0,   0,   0.00,   0, 0, 0.00, 0.00, 0, 0, 1, "0", 0, "2025-02-14"],
], columns=[
    "ac.client_hash",
    "debt_due_bal_ccy_amt", "debt_due_bal_rub_amt",
    "debt_overdue_bal_ccy_amt", "debt_overdue_bal_rub_amt",
    "debt_intr_overdue_bal_ccy_amt", "debt_intr_overdue_bal_rub_amt",
    "debt_tot_os_ccy_amt", "debt_tot_os_rub_amt", "overdue_duration_days",
    "debt_os_max_rub_amt", "debt_ovrd_max_rub_amt", "ovrd_max_dur_days",
    "ovrd_tot_ever_days", "ovrd_tot_entr_ever_qty", "ovrd_max_rub_amt",
    "total_overdue_duration_days", "ovrd_tot_period_qty",
    "ovrd_intr_bal_max_rub_amt", "ovrd_intr_nobal_max_rub_amt",
    "total_overdue_intr_bal_duration_days", "total_overdue_intr_nobal_duration_days",
    "overdue_bucket_id", "overdue_bucket_name", "npl_nflag", "day_part"
])

# Сохраняем как Excel
with pd.ExcelWriter("test-shablon.xlsx", engine="openpyxl") as writer:
    cs.to_excel(writer, sheet_name="cs", index=False)
    c.to_excel(writer, sheet_name="c", index=False)
    tr.to_excel(writer, sheet_name="tr", index=False)
    so.to_excel(writer, sheet_name="so", index=False)
    dog.to_excel(writer, sheet_name="dog", index=False)

print("✅ test-shablon.xlsx создан — все колонки включены")
