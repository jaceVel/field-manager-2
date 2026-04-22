from django.db import migrations


def backfill_plan_numbers(apps, schema_editor):
    JourneyManagementPlan = apps.get_model('reports', 'JourneyManagementPlan')
    for plan in JourneyManagementPlan.objects.filter(plan_number=''):
        plan.plan_number = plan.created_at.strftime('JMP-%Y%m%d-%H%M')
        plan.save(update_fields=['plan_number'])


class Migration(migrations.Migration):

    dependencies = [
        ('reports', '0054_journey_plan_number'),
    ]

    operations = [
        migrations.RunPython(backfill_plan_numbers, migrations.RunPython.noop),
    ]
