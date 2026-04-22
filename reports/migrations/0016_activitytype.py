from django.db import migrations, models

SLUG_TO_NAME = {
    'work': 'Work',
    'standby': 'Standby',
    'downtime': 'Downtime',
    'travel': 'Travel',
    'rdo': 'RDO',
    'mob': 'Mob/Demob',
}

DEFAULTS = [
    ('Work', 0),
    ('Standby', 1),
    ('Downtime', 2),
    ('Travel', 3),
    ('RDO', 4),
    ('Mob/Demob', 5),
]


def seed_types(apps, schema_editor):
    ActivityType = apps.get_model('reports', 'ActivityType')
    for name, order in DEFAULTS:
        ActivityType.objects.get_or_create(name=name, defaults={'order': order})


def convert_slugs_to_names(apps, schema_editor):
    Activity = apps.get_model('reports', 'Activity')
    for slug, name in SLUG_TO_NAME.items():
        Activity.objects.filter(activity_type=slug).update(activity_type=name)


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0015_pssqcpreset'),
    ]

    operations = [
        migrations.CreateModel(
            name='ActivityType',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100, unique=True)),
                ('order', models.PositiveIntegerField(default=0)),
            ],
            options={
                'ordering': ['order', 'name'],
            },
        ),
        migrations.RunPython(seed_types, migrations.RunPython.noop),
        migrations.AlterField(
            model_name='activity',
            name='activity_type',
            field=models.CharField(default='Work', max_length=100),
        ),
        migrations.RunPython(convert_slugs_to_names, migrations.RunPython.noop),
    ]
