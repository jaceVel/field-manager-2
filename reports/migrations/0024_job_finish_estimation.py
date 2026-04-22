from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0023_dailyreport_include_in_avg'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='finish_days_per_week',
            field=models.PositiveSmallIntegerField(default=7),
        ),
        migrations.AddField(
            model_name='job',
            name='finish_rolling_window',
            field=models.PositiveSmallIntegerField(default=7),
        ),
        migrations.AddField(
            model_name='job',
            name='finish_show_linear',
            field=models.BooleanField(default=True),
        ),
        migrations.AddField(
            model_name='job',
            name='finish_show_calendar',
            field=models.BooleanField(default=True),
        ),
        migrations.AddField(
            model_name='job',
            name='finish_show_rolling',
            field=models.BooleanField(default=True),
        ),
    ]
