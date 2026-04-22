# Manually written — splits 3 shared map_color_* fields into 6 separate fields
# (progress_color_* and deployment_color_*)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0021_job_map_color_planned_job_map_color_prev_and_more'),
    ]

    operations = [
        # Add 6 new fields
        migrations.AddField(
            model_name='job',
            name='progress_color_today',
            field=models.CharField(default='#ff9800', max_length=7),
        ),
        migrations.AddField(
            model_name='job',
            name='progress_color_prev',
            field=models.CharField(default='#29b6f6', max_length=7),
        ),
        migrations.AddField(
            model_name='job',
            name='progress_color_planned',
            field=models.CharField(default='#ffffff', max_length=7),
        ),
        migrations.AddField(
            model_name='job',
            name='deployment_color_today',
            field=models.CharField(default='#ff9800', max_length=7),
        ),
        migrations.AddField(
            model_name='job',
            name='deployment_color_prev',
            field=models.CharField(default='#29b6f6', max_length=7),
        ),
        migrations.AddField(
            model_name='job',
            name='deployment_color_planned',
            field=models.CharField(default='#ffffff', max_length=7),
        ),
        # Remove 3 old shared fields
        migrations.RemoveField(
            model_name='job',
            name='map_color_today',
        ),
        migrations.RemoveField(
            model_name='job',
            name='map_color_prev',
        ),
        migrations.RemoveField(
            model_name='job',
            name='map_color_planned',
        ),
    ]
