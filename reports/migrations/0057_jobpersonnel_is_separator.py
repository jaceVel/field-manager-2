from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0056_jobpersonnel_sort_order'),
    ]

    operations = [
        migrations.AddField(
            model_name='jobpersonnel',
            name='is_separator',
            field=models.BooleanField(default=False),
        ),
    ]
