from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0058_job_client_representative'),
    ]

    operations = [
        migrations.AlterField(
            model_name='job',
            name='source_type',
            field=models.CharField(blank=True, max_length=50, verbose_name='Source Type'),
        ),
    ]
