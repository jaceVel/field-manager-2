from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0022_split_map_colors'),
    ]

    operations = [
        migrations.AddField(
            model_name='dailyreport',
            name='include_in_avg',
            field=models.BooleanField(default=True),
        ),
    ]
