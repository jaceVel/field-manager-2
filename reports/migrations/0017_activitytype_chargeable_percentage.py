from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0016_activitytype'),
    ]

    operations = [
        migrations.AddField(
            model_name='activitytype',
            name='chargeable_percentage',
            field=models.PositiveSmallIntegerField(default=100),
        ),
    ]
