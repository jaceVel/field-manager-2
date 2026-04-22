from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0026_dailyreport_include_shot_chart'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='export_filename_template',
            field=models.CharField(default='Daily Production Report-{date}', max_length=200),
        ),
    ]
