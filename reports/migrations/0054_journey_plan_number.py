from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0053_journey_route_map'),
    ]

    operations = [
        migrations.AddField(
            model_name='journeymanagementplan',
            name='plan_number',
            field=models.CharField(blank=True, max_length=50),
        ),
    ]
