from django.core.management.base import BaseCommand
from core.models import Cs, C, Tr, So, Dog

#$ python manage.py check_test_data                                                                                                    

class Command(BaseCommand):
    help = "Проверка наличия тестовых данных в БД"

    def handle(self, *args, **options):
        test_hashes = ['1111111111', '2222222222']

        self.stdout.write(self.style.SUCCESS("=== Проверка тестовых данных ==="))

        # CS
        qs = Cs.objects.filter(ac_client_hash__in=test_hashes)
        self.stdout.write(f"CS: {qs.count()} записей")
        for row in qs.values():
            self.stdout.write(str(row))

        # C
        qs = C.objects.filter(ac_client_hash__in=test_hashes)
        self.stdout.write(f"\nC: {qs.count()} записей")
        for row in qs.values():
            self.stdout.write(str(row))

        # TR
        qs = Tr.objects.filter(ac_client_hash__in=test_hashes)
        self.stdout.write(f"\nTR: {qs.count()} записей")
        for row in qs.values():
            self.stdout.write(str(row))

        # SO
        qs = So.objects.filter(ac_client_hash__in=test_hashes)
        self.stdout.write(f"\nSO: {qs.count()} записей")
        for row in qs.values():
            self.stdout.write(str(row))

        # DOG
        qs = Dog.objects.filter(ac_client_hash__in=test_hashes)
        self.stdout.write(f"\nDOG: {qs.count()} записей")
        for row in qs.values():
            self.stdout.write(str(row))

        self.stdout.write(self.style.SUCCESS("=== Проверка завершена ==="))
