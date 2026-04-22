from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0028_job_export_save_location'),
    ]

    operations = [
        migrations.CreateModel(
            name='JobPersonnel',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100)),
                ('role', models.CharField(blank=True, max_length=100)),
                ('notes', models.CharField(blank=True, max_length=300)),
                ('job', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='personnel', to='reports.job')),
            ],
            options={'ordering': ['role', 'name']},
        ),
        migrations.CreateModel(
            name='JobVehicle',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100)),
                ('vehicle_type', models.CharField(blank=True, max_length=100)),
                ('notes', models.CharField(blank=True, max_length=300)),
                ('job', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='vehicles', to='reports.job')),
            ],
            options={'ordering': ['vehicle_type', 'name']},
        ),
        migrations.CreateModel(
            name='JobEquipment',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100)),
                ('equipment_type', models.CharField(blank=True, max_length=100)),
                ('notes', models.CharField(blank=True, max_length=300)),
                ('job', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='equipment', to='reports.job')),
            ],
            options={'ordering': ['equipment_type', 'name']},
        ),
    ]
