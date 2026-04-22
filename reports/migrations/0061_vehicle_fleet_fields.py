from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0060_jobfieldoption'),
    ]

    operations = [
        migrations.AddField(model_name='vehicle', name='description', field=models.CharField(blank=True, max_length=200)),
        migrations.AddField(model_name='vehicle', name='category', field=models.CharField(blank=True, max_length=100)),
        migrations.AddField(model_name='vehicle', name='make', field=models.CharField(blank=True, max_length=100)),
        migrations.AddField(model_name='vehicle', name='model_name', field=models.CharField(blank=True, max_length=100)),
        migrations.AddField(model_name='vehicle', name='rego', field=models.CharField(blank=True, max_length=30)),
        migrations.AddField(model_name='vehicle', name='is_active', field=models.BooleanField(default=True)),
    ]
