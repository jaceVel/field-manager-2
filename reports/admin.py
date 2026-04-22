from django.contrib import admin
from .models import Job, DailyReport


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display = ['job_number', 'client', 'project_name', 'source_type', 'recording_system']


@admin.register(DailyReport)
class DailyReportAdmin(admin.ModelAdmin):
    list_display = ['job', 'date', 'supervisor', 'status']
    list_filter = ['status', 'job', 'date']


