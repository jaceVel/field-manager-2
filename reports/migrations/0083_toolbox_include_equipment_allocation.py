from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0082_add_watch_fields_to_node_session'),
    ]

    operations = [
        migrations.AddField(
            model_name='toolboxmeeting',
            name='include_equipment_allocation',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='toolboxv2meeting',
            name='include_equipment_allocation',
            field=models.BooleanField(default=False),
        ),
    ]
