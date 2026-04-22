from django.db import migrations, models
import django.db.models.deletion
import reports.models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0085_toolboxv2photo'),
    ]

    operations = [
        migrations.CreateModel(
            name='JobPhoto',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('image', models.ImageField(upload_to=reports.models.job_photo_path)),
                ('caption', models.TextField(blank=True)),
                ('taken_at', models.DateField(null=True, blank=True)),
                ('order', models.PositiveIntegerField(default=0)),
                ('uploaded_at', models.DateTimeField(auto_now_add=True)),
                ('job', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='photos',
                    to='reports.job')),
            ],
            options={'ordering': ['order', 'pk']},
        ),
    ]
