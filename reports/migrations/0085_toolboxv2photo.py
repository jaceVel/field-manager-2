from django.db import migrations, models
import django.db.models.deletion
import reports.models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0084_toolboxv2attendee_is_separator'),
    ]

    operations = [
        migrations.CreateModel(
            name='ToolboxV2Photo',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('image', models.ImageField(upload_to=reports.models.toolbox_v2_photo_path)),
                ('caption', models.TextField(blank=True)),
                ('border_style', models.CharField(
                    choices=[('none', 'None'), ('thin', 'Thin'), ('thick', 'Thick'), ('shadow', 'Shadow')],
                    default='none', max_length=10)),
                ('order', models.PositiveIntegerField(default=0)),
                ('meeting', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='photos',
                    to='reports.toolboxv2meeting')),
            ],
            options={'ordering': ['order', 'pk']},
        ),
    ]
