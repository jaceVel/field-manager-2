from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0073_toolbox_v2_full_fields'),
    ]

    operations = [
        migrations.CreateModel(
            name='JourneyV2Plan',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('plan_number', models.CharField(blank=True, max_length=50)),
                ('departing_from', models.CharField(blank=True, max_length=200)),
                ('depart_date', models.DateField(blank=True, null=True)),
                ('depart_time', models.TimeField(blank=True, null=True)),
                ('depart_contact', models.CharField(blank=True, max_length=100)),
                ('depart_phone', models.CharField(blank=True, max_length=50)),
                ('overnight_location', models.CharField(blank=True, max_length=200)),
                ('overnight_arrival_date', models.DateField(blank=True, null=True)),
                ('overnight_arrival_time', models.TimeField(blank=True, null=True)),
                ('overnight_departure_date', models.DateField(blank=True, null=True)),
                ('overnight_departure_time', models.TimeField(blank=True, null=True)),
                ('arriving_at', models.CharField(blank=True, max_length=200)),
                ('arrive_date', models.DateField(blank=True, null=True)),
                ('arrive_time', models.TimeField(blank=True, null=True)),
                ('arrive_contact', models.CharField(blank=True, max_length=100)),
                ('arrive_phone', models.CharField(blank=True, max_length=50)),
                ('route', models.TextField(blank=True)),
                ('break_journey_at', models.TextField(blank=True)),
                ('radio_channel', models.CharField(blank=True, max_length=50)),
                ('other_instructions', models.TextField(blank=True)),
                ('route_waypoints', models.TextField(blank=True)),
                ('rest_stops_json', models.TextField(blank=True)),
                ('coordinator_name', models.CharField(blank=True, max_length=100)),
                ('coordinator_phone', models.CharField(blank=True, max_length=50)),
                ('plan_communicated', models.BooleanField(blank=True, null=True)),
                ('before_signature', models.TextField(blank=True)),
                ('before_signed_at', models.DateTimeField(blank=True, null=True)),
                ('before_date', models.DateField(blank=True, null=True)),
                ('journey_completed', models.BooleanField(blank=True, null=True)),
                ('after_signature', models.TextField(blank=True)),
                ('after_signed_at', models.DateTimeField(blank=True, null=True)),
                ('after_date', models.DateField(blank=True, null=True)),
                ('action_items', models.TextField(blank=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('job', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='journey_v2_plans', to='reports.job')),
            ],
            options={
                'ordering': ['-created_at'],
            },
        ),
        migrations.CreateModel(
            name='JourneyV2Personnel',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('rego', models.CharField(blank=True, max_length=20)),
                ('name', models.CharField(blank=True, max_length=100)),
                ('is_driver', models.BooleanField(default=False)),
                ('phone', models.CharField(blank=True, max_length=50)),
                ('signature', models.TextField(blank=True)),
                ('signed_at', models.DateTimeField(blank=True, null=True)),
                ('order', models.PositiveIntegerField(default=0)),
                ('plan', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='personnel', to='reports.journeyv2plan')),
            ],
            options={
                'ordering': ['order', 'pk'],
            },
        ),
    ]
