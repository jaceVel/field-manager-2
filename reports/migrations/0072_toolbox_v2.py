from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0071_restructure_take5_hazard'),
    ]

    operations = [
        migrations.CreateModel(
            name='ToolboxV2Meeting',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date', models.DateField(blank=True, null=True)),
                ('time', models.TimeField(blank=True, null=True)),
                ('location', models.CharField(blank=True, max_length=200)),
                ('supervisor', models.CharField(blank=True, max_length=100)),
                ('topics', models.TextField(blank=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('job', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='toolbox_v2_meetings', to='reports.job')),
            ],
            options={
                'ordering': ['-date', '-created_at'],
            },
        ),
        migrations.CreateModel(
            name='ToolboxV2Attendee',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=100)),
                ('role', models.CharField(blank=True, max_length=200)),
                ('signature', models.TextField(blank=True)),
                ('signed_at', models.DateTimeField(blank=True, null=True)),
                ('order', models.PositiveIntegerField(default=0)),
                ('meeting', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='attendees', to='reports.toolboxv2meeting')),
            ],
            options={
                'ordering': ['order', 'pk'],
            },
        ),
    ]
