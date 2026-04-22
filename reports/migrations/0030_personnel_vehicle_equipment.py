from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0029_jobpersonnel_jobvehicle_jobequipment'),
    ]

    operations = [
        migrations.CreateModel(
            name='Personnel',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100)),
                ('role', models.CharField(blank=True, max_length=100)),
                ('notes', models.CharField(blank=True, max_length=300)),
            ],
            options={'ordering': ['role', 'name']},
        ),
        migrations.CreateModel(
            name='Vehicle',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100)),
                ('vehicle_type', models.CharField(blank=True, max_length=100)),
                ('notes', models.CharField(blank=True, max_length=300)),
            ],
            options={'ordering': ['vehicle_type', 'name']},
        ),
        migrations.CreateModel(
            name='Equipment',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100)),
                ('equipment_type', models.CharField(blank=True, max_length=100)),
                ('notes', models.CharField(blank=True, max_length=300)),
            ],
            options={'ordering': ['equipment_type', 'name']},
        ),
    ]
