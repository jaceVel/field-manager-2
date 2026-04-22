from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0031_personnel_skills'),
    ]

    operations = [
        migrations.RemoveField(model_name='personnel', name='skills'),
        migrations.AddField(
            model_name='personnel',
            name='phone',
            field=models.CharField(blank=True, max_length=50),
        ),
        migrations.AddField(
            model_name='personnel',
            name='email',
            field=models.EmailField(blank=True, max_length=200),
        ),
    ]
