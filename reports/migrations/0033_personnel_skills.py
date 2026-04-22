from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0032_personnel_phone_email'),
    ]

    operations = [
        migrations.AddField(
            model_name='personnel',
            name='skills',
            field=models.ManyToManyField(blank=True, related_name='personnel', to='reports.skill'),
        ),
    ]
