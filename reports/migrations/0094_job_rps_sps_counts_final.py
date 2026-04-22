from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0093_job_rps_sps_files'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='rps_count',
            field=models.PositiveIntegerField(blank=True, null=True, verbose_name='RPS Record Count'),
        ),
        migrations.AddField(
            model_name='job',
            name='rps_is_final',
            field=models.BooleanField(default=False, verbose_name='RPS Final File'),
        ),
        migrations.AddField(
            model_name='job',
            name='sps_count',
            field=models.PositiveIntegerField(blank=True, null=True, verbose_name='SPS Record Count'),
        ),
        migrations.AddField(
            model_name='job',
            name='sps_is_final',
            field=models.BooleanField(default=False, verbose_name='SPS Final File'),
        ),
    ]
