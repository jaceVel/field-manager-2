from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0095_dailyreport_active_patch'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='survey_epsg',
            field=models.CharField(blank=True, max_length=20, null=True, verbose_name='Survey EPSG Code'),
        ),
    ]
