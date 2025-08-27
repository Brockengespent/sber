from rest_framework import serializers
from core.models import Dog

class ClientDebtSerializer(serializers.ModelSerializer):
    client_id = serializers.CharField(source='ac_client_hash')
    total_debt = serializers.DecimalField(source='debt_tot_os_rub_amt', max_digits=18, decimal_places=2)
    overdue_bucket = serializers.CharField(source='overdue_bucket_name')

    class Meta:
        model = Dog
        fields = ('client_id', 'total_debt', 'overdue_bucket', 'npl_nflag')
