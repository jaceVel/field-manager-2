from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0017_activitytype_chargeable_percentage'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='timezone',
            field=models.CharField(
                choices=[
                    ('Australia/Perth', 'Australia/Perth (UTC+8)'),
                    ('Australia/Darwin', 'Australia/Darwin (UTC+9:30)'),
                    ('Australia/Adelaide', 'Australia/Adelaide (UTC+9:30/10:30)'),
                    ('Australia/Brisbane', 'Australia/Brisbane (UTC+10)'),
                    ('Australia/Sydney', 'Australia/Sydney (UTC+10/11)'),
                    ('Australia/Melbourne', 'Australia/Melbourne (UTC+10/11)'),
                    ('Australia/Hobart', 'Australia/Hobart (UTC+10/11)'),
                    ('UTC', 'UTC'),
                    ('Asia/Kuala_Lumpur', 'Malaysia (UTC+8)'),
                    ('Asia/Jakarta', 'Indonesia West (UTC+7)'),
                    ('Asia/Makassar', 'Indonesia Central (UTC+8)'),
                    ('Asia/Jayapura', 'Indonesia East (UTC+9)'),
                    ('America/Denver', 'US Mountain (UTC-7/-6)'),
                    ('America/Chicago', 'US Central (UTC-6/-5)'),
                    ('America/New_York', 'US Eastern (UTC-5/-4)'),
                ],
                default='Australia/Perth',
                max_length=50,
                verbose_name='Job Timezone',
            ),
        ),
    ]
