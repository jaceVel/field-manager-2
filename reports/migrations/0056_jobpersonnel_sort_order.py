from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0055_backfill_plan_numbers'),
    ]

    operations = [
        migrations.AddField(
            model_name='jobpersonnel',
            name='sort_order',
            field=models.PositiveIntegerField(default=0),
        ),
    ]
