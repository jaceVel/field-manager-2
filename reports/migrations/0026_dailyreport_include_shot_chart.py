from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0025_job_finish_include_in_report'),
    ]

    operations = [
        migrations.AddField(
            model_name='dailyreport',
            name='include_shot_chart',
            field=models.BooleanField(default=False),
        ),
    ]
