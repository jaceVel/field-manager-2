from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0094_job_rps_sps_counts_final'),
    ]

    operations = [
        migrations.AddField(
            model_name='dailyreport',
            name='last_line_in_ground',
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True),
        ),
        migrations.AddField(
            model_name='dailyreport',
            name='last_station_in_ground',
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True),
        ),
    ]
