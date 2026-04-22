from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0064_jobequipment_assigned_vehicle'),
    ]

    operations = [
        # Add tab to VehicleAllocation
        migrations.AddField(
            model_name='vehicleallocation',
            name='tab',
            field=models.CharField(default='job', max_length=10),
        ),
        migrations.AlterUniqueTogether(
            name='vehicleallocation',
            unique_together={('job_vehicle', 'person_name', 'tab')},
        ),
        # Remove assigned_vehicle from JobEquipment
        migrations.RemoveField(
            model_name='jobequipment',
            name='assigned_vehicle',
        ),
        # Create JobEquipmentVehicleLink
        migrations.CreateModel(
            name='JobEquipmentVehicleLink',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('tab', models.CharField(default='job', max_length=10)),
                ('job_equipment', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE,
                                                    related_name='vehicle_links', to='reports.jobequipment')),
                ('job_vehicle', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE,
                                                  related_name='equipment_links', to='reports.jobvehicle')),
            ],
            options={
                'unique_together': {('job_equipment', 'tab')},
            },
        ),
    ]
