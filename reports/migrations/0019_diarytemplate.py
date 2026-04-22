from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0018_job_timezone'),
    ]

    operations = [
        migrations.CreateModel(
            name='DiaryTemplate',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100)),
                ('body', models.TextField()),
                ('order', models.PositiveIntegerField(default=0)),
            ],
            options={
                'ordering': ['order', 'name'],
            },
        ),
    ]
