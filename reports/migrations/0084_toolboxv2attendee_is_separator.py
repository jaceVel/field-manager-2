from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0083_toolbox_include_equipment_allocation'),
    ]

    operations = [
        migrations.AddField(
            model_name='toolboxv2attendee',
            name='is_separator',
            field=models.BooleanField(default=False),
        ),
    ]
