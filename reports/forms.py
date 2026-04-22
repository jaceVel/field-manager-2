from django import forms
from .models import Job, DailyReport, SurveyFile


class JobForm(forms.ModelForm):
    class Meta:
        model = Job
        fields = ['job_number', 'client', 'client_representative', 'project_name', 'source_type', 'recording_system', 'rx_interval', 'sx_interval', 'timezone', 'take5_pin']


class SurveyFileForm(forms.ModelForm):
    class Meta:
        model = SurveyFile
        fields = ['file_type', 'datum', 'zone', 'file']


class DailyReportForm(forms.ModelForm):
    class Meta:
        model = DailyReport
        fields = ['date']
        widgets = {
            'date': forms.DateInput(attrs={'type': 'date'}),
        }
