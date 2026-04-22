from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0061_vehicle_fleet_fields'),
    ]

    operations = [
        migrations.CreateModel(
            name='VehicleAllocation',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('person_name', models.CharField(max_length=200)),
                ('job_vehicle', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='allocations', to='reports.jobvehicle')),
            ],
            options={
                'unique_together': {('job_vehicle', 'person_name')},
            },
        ),
    ]
