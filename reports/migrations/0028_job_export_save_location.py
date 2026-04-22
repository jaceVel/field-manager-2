from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0027_job_export_filename_template'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='export_save_to_disk',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='job',
            name='export_save_path',
            field=models.CharField(blank=True, default='', max_length=500),
        ),
    ]
