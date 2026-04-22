from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0059_alter_job_source_type'),
    ]

    operations = [
        migrations.CreateModel(
            name='JobFieldOption',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('field', models.CharField(choices=[('recording_system', 'Recording System'), ('source_type', 'Source Type')], max_length=50)),
                ('value', models.CharField(max_length=200)),
            ],
            options={
                'ordering': ['value'],
                'unique_together': {('field', 'value')},
            },
        ),
    ]
