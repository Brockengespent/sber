# core/management/commands/reset_data.py
from django.core.management.base import BaseCommand
from django.db import transaction
from core.models import Cs, C, Tr, So, Dog, ClientCity

class Command(BaseCommand):
    help = "Очистить данные моделей (без прямого TRUNCATE для view)"

    @transaction.atomic
    def handle(self, *args, **opts):
        # порядок важен из-за внешних ключей
        Cs.objects.all().delete()
        So.objects.all().delete()
        Tr.objects.all().delete()
        C.objects.all().delete()
        Dog.objects.all().delete()
        # ClientCity может быть view — чистим только если это реальная таблица
        try:
            ClientCity.objects.all().delete()
        except Exception:
            self.stdout.write(self.style.WARNING("Пропущено ClientCity (не таблица)."))
        self.stdout.write(self.style.SUCCESS("Данные очищены (ORM)."))
