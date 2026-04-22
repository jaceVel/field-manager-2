from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0030_personnel_vehicle_equipment'),
    ]

    operations = [
        migrations.CreateModel(
            name='Skill',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100, unique=True)),
            ],
            options={'ordering': ['name']},
        ),
        migrations.RemoveField(model_name='personnel', name='role'),
        migrations.RemoveField(model_name='personnel', name='notes'),
        migrations.AlterModelOptions('personnel', options={'ordering': ['name']}),
        migrations.AddField(
            model_name='personnel',
            name='skills',
            field=models.ManyToManyField(blank=True, related_name='personnel', to='reports.skill'),
        ),
    ]
