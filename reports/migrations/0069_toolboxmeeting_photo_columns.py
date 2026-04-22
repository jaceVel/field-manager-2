from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0068_add_toolbox_photo'),
    ]

    operations = [
        migrations.AddField(
            model_name='toolboxmeeting',
            name='photo_columns',
            field=models.IntegerField(default=2),
        ),
    ]
