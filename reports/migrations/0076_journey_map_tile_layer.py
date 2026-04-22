from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0075_journey_include_map'),
    ]

    operations = [
        migrations.AddField(
            model_name='journeymanagementplan',
            name='map_tile_layer',
            field=models.CharField(default='street', max_length=20),
        ),
        migrations.AddField(
            model_name='journeyv2plan',
            name='map_tile_layer',
            field=models.CharField(default='street', max_length=20),
        ),
    ]
