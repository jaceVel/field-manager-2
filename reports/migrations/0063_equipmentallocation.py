from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0062_vehicleallocation'),
    ]

    operations = [
        migrations.CreateModel(
            name='EquipmentAllocation',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('person_name', models.CharField(max_length=200)),
                ('job_equipment', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='allocations', to='reports.jobequipment')),
            ],
            options={
                'unique_together': {('job_equipment', 'person_name')},
            },
        ),
    ]
