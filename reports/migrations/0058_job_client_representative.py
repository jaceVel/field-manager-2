from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0057_jobpersonnel_is_separator'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='client_representative',
            field=models.CharField(blank=True, max_length=200, verbose_name='Client Representative'),
        ),
    ]
