from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0024_job_finish_estimation'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='finish_include_in_report',
            field=models.BooleanField(default=False),
        ),
    ]
