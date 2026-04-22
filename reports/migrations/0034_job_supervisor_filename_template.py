from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0033_personnel_skills'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='supervisor_filename_template',
            field=models.CharField(default='Supervisors Daily Report-{date}', max_length=200),
        ),
    ]
