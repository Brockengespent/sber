from django.db.models import F
from rest_framework.generics import ListAPIView
from rest_framework.pagination import PageNumberPagination
from core.models import Dog
from .serializers import ClientDebtSerializer

class ClientPagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 200

class ClientsListAPI(ListAPIView):
    serializer_class = ClientDebtSerializer
    pagination_class = ClientPagination

    def get_queryset(self):
        qs = Dog.objects.all().only(
            'id', 'ac_client_hash', 'debt_tot_os_rub_amt', 'overdue_bucket_name', 'npl_nflag'
        )

        # фильтр по сумме
        debt_min = self.request.query_params.get('debt_min')
        debt_max = self.request.query_params.get('debt_max')
        if debt_min not in (None, ''):
            qs = qs.filter(debt_tot_os_rub_amt__gte=debt_min)
        if debt_max not in (None, ''):
            qs = qs.filter(debt_tot_os_rub_amt__lte=debt_max)

        # фильтр по бакетам
        buckets = self.request.query_params.getlist('bucket')
        if buckets:
            qs = qs.filter(overdue_bucket_name__in=buckets)

        # DISTINCT ON: одна запись на клиента с макс. долгом
        qs = qs.order_by('ac_client_hash', '-debt_tot_os_rub_amt', 'id').distinct('ac_client_hash')

        # итоговая сортировка для пользователя
        ordering = self.request.query_params.get('ordering', '-total_debt')
        ordering_map = {
            'total_debt': 'debt_tot_os_rub_amt',
            '-total_debt': '-debt_tot_os_rub_amt',
        }
        qs = qs.order_by(ordering_map.get(ordering, '-debt_tot_os_rub_amt'))

        return qs

