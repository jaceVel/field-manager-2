from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0090_add_include_signatures'),
    ]

    operations = [
        migrations.AddField(
            model_name='equipment',
            name='serial_number',
            field=models.CharField(blank=True, max_length=100),
        ),
        migrations.AddField(
            model_name='equipment',
            name='is_active',
            field=models.BooleanField(default=True),
        ),
    ]
