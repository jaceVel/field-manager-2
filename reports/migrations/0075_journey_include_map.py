from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0074_journey_v2'),
    ]

    operations = [
        migrations.AddField(
            model_name='journeymanagementplan',
            name='include_map_in_pdf',
            field=models.BooleanField(default=True),
        ),
        migrations.AddField(
            model_name='journeyv2plan',
            name='include_map_in_pdf',
            field=models.BooleanField(default=True),
        ),
    ]
