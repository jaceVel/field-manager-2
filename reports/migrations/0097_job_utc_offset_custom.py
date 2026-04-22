from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0096_job_survey_epsg'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='utc_offset_custom',
            field=models.DecimalField(blank=True, decimal_places=1, max_digits=4, null=True, verbose_name='Custom UTC Offset (hrs)'),
        ),
    ]
