from django.db import models

# 1) cs
class Cs(models.Model):
    ac_client_hash = models.CharField(max_length=50, db_column='ac.client_hash')
    eventaction = models.CharField(max_length=100)
    geolatitude = models.FloatField(null=True, blank=True)
    geolongitude = models.FloatField(null=True, blank=True)
    dt = models.DateTimeField()
    date_part = models.DateField()

    class Meta:
        db_table = 'cs'
        managed = False

# 2) c
class C(models.Model):
    src = models.CharField(max_length=10)
    ac_client_hash = models.CharField(max_length=50, db_column='ac.client_hash')
    c_txn_dt = models.DateTimeField()
    txn_cod_type_rk = models.IntegerField()
    txn_cod_type_name = models.CharField(max_length=200)
    c_txn_rub_amt = models.DecimalField(max_digits=18, decimal_places=2)
    pmnt_payer_name = models.CharField(max_length=200, null=True, blank=True)
    day_part = models.DateField()

    class Meta:
        db_table = 'c'
        managed = False
#3) Tr
class Tr(models.Model):
    id = models.BigAutoField(primary_key=True)
    src = models.CharField(max_length=10, db_column='t_src')
    ac_client_hash = models.CharField(max_length=50, db_column='t_client_hash')
    c_txn_dt = models.DateTimeField(db_column='t_evt_posted_dttm')
    t_trx_city = models.CharField(max_length=100, null=True, blank=True, db_column='t_trx_city')
    txn_cod_type_rk = models.IntegerField(null=True, blank=True, db_column='t_mcc_code')
    t_trans_type = models.IntegerField(null=True, blank=True, db_column='t_trans_type')
    t_trx_direction = models.CharField(max_length=5, null=True, blank=True)
    t_merchant_id = models.CharField(max_length=50, null=True, blank=True)
    t_terminal_id = models.CharField(max_length=50, null=True, blank=True)
    t_merchant_name = models.CharField(max_length=255, null=True, blank=True)
    c_txn_rub_amt = models.DecimalField(max_digits=18, decimal_places=4, null=True, blank=True, db_column='t_amt')
    day_part = models.DateField()

    class Meta:
        db_table = 'tr'
        managed = False




# 4) so
class So(models.Model):
    ac_client_hash = models.CharField(max_length=50, db_column='ac.client_hash')
    erib_id = models.CharField(max_length=100)
    oper_rur_amt = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    login_type = models.CharField(max_length=50)
    oper_type = models.CharField(max_length=50)
    date_time_oper = models.DateTimeField()
    date_create = models.DateField()
    date_time_create = models.DateTimeField()
    doc_type = models.CharField(max_length=50)
    receiver_client_hash = models.CharField(max_length=50, null=True, blank=True)
    t_p2p_flg = models.BooleanField(db_column='t.p2p_flg')
    day_part = models.DateField()

    class Meta:
        db_table = 'so'
        managed = False

# 5) dog — в БД таблица называется core_dog
class Dog(models.Model):
    id = models.BigAutoField(primary_key=True, db_column='id')
    ac_client_hash = models.BigIntegerField(db_column='ac_client_hash')
    debt_due_bal_ccy_amt = models.DecimalField(max_digits=18, decimal_places=2, db_column='debt_due_bal_ccy_amt', null=True)
    debt_due_bal_rub_amt = models.DecimalField(max_digits=18, decimal_places=2, db_column='debt_due_bal_rub_amt', null=True)
    debt_overdue_bal_ccy_amt = models.DecimalField(max_digits=18, decimal_places=2, db_column='debt_overdue_bal_ccy_amt', null=True)
    debt_overdue_bal_rub_amt = models.DecimalField(max_digits=18, decimal_places=2, db_column='debt_overdue_bal_rub_amt', null=True)
    debt_intr_overdue_bal_ccy_amt = models.DecimalField(max_digits=18, decimal_places=2, db_column='debt_intr_overdue_bal_ccy_amt', null=True)
    debt_intr_overdue_bal_rub_amt = models.DecimalField(max_digits=18, decimal_places=2, db_column='debt_intr_overdue_bal_rub_amt', null=True)
    debt_tot_os_ccy_amt = models.DecimalField(max_digits=18, decimal_places=2, db_column='debt_tot_os_ccy_amt', null=True)
    debt_tot_os_rub_amt = models.DecimalField(max_digits=18, decimal_places=2, db_column='debt_tot_os_rub_amt', null=True)
    overdue_duration_days = models.IntegerField(db_column='overdue_duration_days', null=True)
    debt_os_max_rub_amt = models.DecimalField(max_digits=18, decimal_places=2, db_column='debt_os_max_rub_amt', null=True)
    debt_ovrd_max_rub_amt = models.DecimalField(max_digits=18, decimal_places=2, db_column='debt_ovrd_max_rub_amt', null=True)
    ovrd_max_dur_days = models.IntegerField(db_column='ovrd_max_dur_days', null=True)
    ovrd_tot_ever_days = models.IntegerField(db_column='ovrd_tot_ever_days', null=True)
    ovrd_tot_entr_ever_qty = models.IntegerField(db_column='ovrd_tot_entr_ever_qty', null=True)
    ovrd_max_rub_amt = models.DecimalField(max_digits=18, decimal_places=2, db_column='ovrd_max_rub_amt', null=True)
    total_overdue_duration_days = models.IntegerField(db_column='total_overdue_duration_days', null=True)
    ovrd_tot_period_qty = models.IntegerField(db_column='ovrd_tot_period_qty', null=True)
    ovrd_intr_bal_max_rub_amt = models.DecimalField(max_digits=18, decimal_places=2, db_column='ovrd_intr_bal_max_rub_amt', null=True)
    ovrd_intr_nobal_max_rub_amt = models.DecimalField(max_digits=18, decimal_places=2, db_column='ovrd_intr_nobal_max_rub_amt', null=True)
    total_overdue_intr_bal_duration_days = models.IntegerField(db_column='total_overdue_intr_bal_duration_days', null=True)
    total_overdue_intr_nobal_duration_days = models.IntegerField(db_column='total_overdue_intr_nobal_duration_days', null=True)
    overdue_bucket_id = models.IntegerField(db_column='overdue_bucket_id', null=True)
    overdue_bucket_name = models.CharField(max_length=50, db_column='overdue_bucket_name', null=True)
    npl_nflag = models.BooleanField(db_column='npl_nflag', null=True)
    day_part = models.DateField(db_column='day_part', null=True)

    class Meta:
        db_table = 'core_dog'
        managed = False

# 6) clients_city 
class ClientCity(models.Model):
    ac_client_hash = models.BigIntegerField(primary_key=True, db_column='ac_client_hash')
    city = models.CharField(max_length=100, null=True, blank=True, db_column='city')

    class Meta:
        db_table = 'clients_city'
        managed = False


