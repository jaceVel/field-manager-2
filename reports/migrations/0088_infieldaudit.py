from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0087_tasksafetyobservation'),
    ]

    operations = [
        migrations.CreateModel(
            name='InfieldAudit',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date_commenced', models.DateField(blank=True, null=True)),
                ('date_completed', models.DateField(blank=True, null=True)),
                ('site_location', models.CharField(blank=True, max_length=200)),
                ('audit_conducted_by', models.CharField(blank=True, max_length=100)),
                ('crew_supervisor', models.CharField(blank=True, max_length=100)),
                ('checklist', models.JSONField(blank=True, default=dict)),
                ('actions', models.JSONField(blank=True, default=list)),
                ('signature', models.TextField(blank=True)),
                ('signed_at', models.DateTimeField(blank=True, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('job', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='infield_audits', to='reports.job')),
            ],
            options={
                'ordering': ['-date_commenced', '-created_at'],
            },
        ),
    ]
