from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0086_jobphoto'),
    ]

    operations = [
        migrations.CreateModel(
            name='TaskSafetyObservation',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('task_being_observed', models.CharField(blank=True, max_length=200)),
                ('date', models.DateField(blank=True, null=True)),
                ('time', models.TimeField(blank=True, null=True)),
                ('observer', models.CharField(blank=True, max_length=100)),
                ('location', models.CharField(blank=True, max_length=200)),
                ('team_members', models.JSONField(blank=True, default=list)),
                ('checklist', models.JSONField(blank=True, default=dict)),
                ('at_risk', models.CharField(blank=True, max_length=3)),
                ('discussion', models.TextField(blank=True)),
                ('signature', models.TextField(blank=True)),
                ('signed_at', models.DateTimeField(blank=True, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('job', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='task_observations', to='reports.job')),
            ],
            options={
                'ordering': ['-date', '-created_at'],
            },
        ),
    ]
