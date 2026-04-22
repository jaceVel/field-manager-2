from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0088_infieldaudit'),
    ]

    operations = [
        migrations.CreateModel(
            name='JSA',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('jsa_name', models.CharField(blank=True, max_length=300)),
                ('reference_tp', models.CharField(blank=True, max_length=100)),
                ('reference_tra', models.CharField(blank=True, max_length=100)),
                ('project', models.CharField(blank=True, max_length=200)),
                ('site', models.CharField(blank=True, max_length=200)),
                ('date', models.DateField(blank=True, null=True)),
                ('time', models.TimeField(blank=True, null=True)),
                ('job_description', models.TextField(blank=True)),
                ('tools_equipment', models.TextField(blank=True)),
                ('ppe_required', models.TextField(blank=True)),
                ('permits_approvals', models.TextField(blank=True)),
                ('participants', models.JSONField(blank=True, default=list)),
                ('approver_name', models.CharField(blank=True, max_length=100)),
                ('approver_position', models.CharField(blank=True, max_length=100)),
                ('approver_signature', models.TextField(blank=True)),
                ('approval_date', models.DateField(blank=True, null=True)),
                ('approval_signed_at', models.DateTimeField(blank=True, null=True)),
                ('analysis', models.JSONField(blank=True, default=list)),
                ('photo1', models.TextField(blank=True)),
                ('photo2', models.TextField(blank=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('job', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='jsas', to='reports.job')),
            ],
            options={
                'ordering': ['-date', '-created_at'],
            },
        ),
    ]
