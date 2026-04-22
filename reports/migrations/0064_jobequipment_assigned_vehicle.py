from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0063_equipmentallocation'),
    ]

    operations = [
        migrations.AddField(
            model_name='jobequipment',
            name='assigned_vehicle',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL,
                                    related_name='equipment_items', to='reports.jobvehicle'),
        ),
    ]
