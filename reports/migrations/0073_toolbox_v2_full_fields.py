from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0072_toolbox_v2'),
    ]

    operations = [
        # ToolboxV2Meeting — add all fields to match ToolboxMeeting
        migrations.AddField('ToolboxV2Meeting', 'meeting_type',
            models.CharField(choices=[('daily', 'Daily Toolbox Meeting'), ('jmp', 'Journey Management Toolbox')],
                             default='daily', max_length=10)),
        migrations.AddField('ToolboxV2Meeting', 'days_on_job',
            models.CharField(blank=True, max_length=20)),
        migrations.AddField('ToolboxV2Meeting', 'jmp_number',
            models.CharField(blank=True, max_length=50)),
        migrations.AddField('ToolboxV2Meeting', 'yesterday_activities',
            models.TextField(blank=True)),
        migrations.AddField('ToolboxV2Meeting', 'jmp_route_discussed',
            models.BooleanField(default=False)),
        migrations.AddField('ToolboxV2Meeting', 'jmp_hours_noted',
            models.BooleanField(default=False)),
        migrations.AddField('ToolboxV2Meeting', 'jmp_contact_numbers',
            models.BooleanField(default=False)),
        migrations.AddField('ToolboxV2Meeting', 'jmp_signed_off',
            models.BooleanField(default=False)),
        migrations.AddField('ToolboxV2Meeting', 'jmp_lead_tail',
            models.BooleanField(default=False)),
        migrations.AddField('ToolboxV2Meeting', 'todays_activities',
            models.TextField(blank=True)),
        migrations.AddField('ToolboxV2Meeting', 'terrain_discussion',
            models.TextField(blank=True)),
        migrations.AddField('ToolboxV2Meeting', 'road_condition',
            models.TextField(blank=True)),
        migrations.AddField('ToolboxV2Meeting', 'muster_point',
            models.CharField(blank=True, max_length=200)),
        migrations.AddField('ToolboxV2Meeting', 'forecast',
            models.TextField(blank=True)),
        migrations.AddField('ToolboxV2Meeting', 'uv_index',
            models.CharField(blank=True, choices=[('low', 'Low'), ('high', 'High'), ('extreme', 'Extreme')], max_length=10)),
        migrations.AddField('ToolboxV2Meeting', 'chance_of_rain',
            models.CharField(blank=True, max_length=50)),
        migrations.AddField('ToolboxV2Meeting', 'min_temp',
            models.CharField(blank=True, max_length=20)),
        migrations.AddField('ToolboxV2Meeting', 'max_temp',
            models.CharField(blank=True, max_length=20)),
        migrations.AddField('ToolboxV2Meeting', 'grass_fire',
            models.CharField(blank=True, choices=[('low', 'Low'), ('high', 'High'), ('extreme', 'Extreme')], max_length=10)),
        migrations.AddField('ToolboxV2Meeting', 'forest_fire',
            models.CharField(blank=True, choices=[('low', 'Low'), ('high', 'High'), ('extreme', 'Extreme')], max_length=10)),
        migrations.AddField('ToolboxV2Meeting', 'wind',
            models.CharField(blank=True, max_length=100)),
        # Rename topics → other_topics
        migrations.RenameField('ToolboxV2Meeting', 'topics', 'other_topics'),
        migrations.AddField('ToolboxV2Meeting', 'photo_columns',
            models.IntegerField(default=2)),

        # ToolboxV2Attendee — rename role → job_role, add vehicle + bac
        migrations.RenameField('ToolboxV2Attendee', 'role', 'job_role'),
        migrations.AddField('ToolboxV2Attendee', 'vehicle',
            models.CharField(blank=True, max_length=100)),
        migrations.AddField('ToolboxV2Attendee', 'bac',
            models.CharField(blank=True, max_length=20)),
    ]
