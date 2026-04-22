import json
from django.db import models
from django.contrib.auth.models import User


class Job(models.Model):
    SOURCE_CHOICES = [
        ('vibroseis', 'Vibroseis'),
        ('dynamite', 'Dynamite'),
        ('other', 'Other'),
    ]

    TIMEZONE_CHOICES = [
        ('Australia/Perth',    'Australia/Perth (UTC+8)'),
        ('Australia/Darwin',   'Australia/Darwin (UTC+9:30)'),
        ('Australia/Adelaide', 'Australia/Adelaide (UTC+9:30/10:30)'),
        ('Australia/Brisbane', 'Australia/Brisbane (UTC+10)'),
        ('Australia/Sydney',   'Australia/Sydney (UTC+10/11)'),
        ('Australia/Melbourne','Australia/Melbourne (UTC+10/11)'),
        ('Australia/Hobart',   'Australia/Hobart (UTC+10/11)'),
        ('UTC',                'UTC'),
        ('Asia/Kuala_Lumpur',  'Malaysia (UTC+8)'),
        ('Asia/Jakarta',       'Indonesia West (UTC+7)'),
        ('Asia/Makassar',      'Indonesia Central (UTC+8)'),
        ('Asia/Jayapura',      'Indonesia East (UTC+9)'),
        ('America/Denver',     'US Mountain (UTC-7/-6)'),
        ('America/Chicago',    'US Central (UTC-6/-5)'),
        ('America/New_York',   'US Eastern (UTC-5/-4)'),
        ('custom',             'Custom UTC offset (hrs)...'),
    ]

    job_number = models.CharField(max_length=20, unique=True)
    client = models.CharField(max_length=200)
    client_representative = models.CharField(max_length=200, blank=True, verbose_name='Client Representative')
    project_name = models.CharField(max_length=200)
    source_type = models.CharField(max_length=50, blank=True, verbose_name='Source Type')
    recording_system = models.CharField(max_length=100)
    rx_interval = models.DecimalField(max_digits=6, decimal_places=1, null=True, blank=True, verbose_name='Receiver Interval (m)')
    sx_interval = models.DecimalField(max_digits=6, decimal_places=1, null=True, blank=True, verbose_name='Source Interval (m)')
    estimated_rx_count = models.PositiveIntegerField(null=True, blank=True, verbose_name='Estimated Rx Count')
    estimated_sx_count = models.PositiveIntegerField(null=True, blank=True, verbose_name='Estimated Sx Count')
    timezone = models.CharField(max_length=50, choices=TIMEZONE_CHOICES, default='Australia/Perth', verbose_name='Job Timezone')
    utc_offset_custom = models.DecimalField(max_digits=4, decimal_places=1, null=True, blank=True, verbose_name='Custom UTC Offset (hrs)')

    # Map colors — Shot Progress
    progress_color_today   = models.CharField(max_length=7, default='#ff9800')
    progress_color_prev    = models.CharField(max_length=7, default='#29b6f6')
    progress_color_planned = models.CharField(max_length=7, default='#ffffff')
    # Map colors — Node Deployment Progress
    deployment_color_today   = models.CharField(max_length=7, default='#ff9800')
    deployment_color_prev    = models.CharField(max_length=7, default='#29b6f6')
    deployment_color_planned = models.CharField(max_length=7, default='#ffffff')
    # Map overlay
    show_map_overlay = models.BooleanField(default=False)

    # Export filename template and save location
    export_filename_template = models.CharField(
        max_length=200,
        default='Daily Production Report-{date}',
    )
    supervisor_filename_template = models.CharField(
        max_length=200,
        default='Supervisors Daily Report-{date}',
    )
    export_save_to_disk = models.BooleanField(default=False)
    export_save_path = models.CharField(max_length=500, blank=True, default='')

    # Finish date estimation
    finish_days_per_week    = models.PositiveSmallIntegerField(default=7)
    finish_rolling_window   = models.PositiveSmallIntegerField(default=7)
    finish_show_linear      = models.BooleanField(default=True)
    finish_show_calendar    = models.BooleanField(default=True)
    finish_show_rolling     = models.BooleanField(default=True)
    finish_include_in_report = models.BooleanField(default=False)

    # Signature section
    include_signatures = models.BooleanField(default=False)

    # PSS QC thresholds
    pss_force_avg_green = models.FloatField(default=55)
    pss_force_avg_amber = models.FloatField(default=50)
    pss_force_max_green = models.FloatField(default=72)
    pss_force_max_amber = models.FloatField(default=70)
    pss_phase_avg_green = models.FloatField(default=3.6)
    pss_phase_avg_amber = models.FloatField(default=4.0)
    pss_phase_max_green = models.FloatField(default=9)
    pss_phase_max_amber = models.FloatField(default=10)
    pss_thd_avg_green = models.FloatField(default=18)
    pss_thd_avg_amber = models.FloatField(default=20)
    pss_thd_max_green = models.FloatField(default=45)
    pss_thd_max_amber = models.FloatField(default=50)

    take5_pin = models.CharField(max_length=20, blank=True, verbose_name='Take 5 PIN')

    # Survey position files (job-level, shared across all reports)
    rps_file = models.FileField(upload_to='survey_positions/', null=True, blank=True, verbose_name='RPS File')
    rps_count = models.PositiveIntegerField(null=True, blank=True, verbose_name='RPS Record Count')
    rps_is_final = models.BooleanField(default=False, verbose_name='RPS Final File')
    sps_file = models.FileField(upload_to='survey_positions/', null=True, blank=True, verbose_name='SPS File')
    sps_count = models.PositiveIntegerField(null=True, blank=True, verbose_name='SPS Record Count')
    sps_is_final = models.BooleanField(default=False, verbose_name='SPS Final File')
    survey_epsg = models.CharField(max_length=20, null=True, blank=True, verbose_name='Survey EPSG Code')

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.job_number} — {self.client} {self.project_name}"


class JobFieldOption(models.Model):
    FIELD_CHOICES = [
        ('recording_system', 'Recording System'),
        ('source_type', 'Source Type'),
    ]
    field = models.CharField(max_length=50, choices=FIELD_CHOICES)
    value = models.CharField(max_length=200)

    class Meta:
        unique_together = ['field', 'value']
        ordering = ['value']

    def __str__(self):
        return f"{self.field}: {self.value}"


class PSSQCPreset(models.Model):
    name = models.CharField(max_length=100, unique=True)
    pss_force_avg_green = models.FloatField(default=55)
    pss_force_avg_amber = models.FloatField(default=50)
    pss_force_max_green = models.FloatField(default=72)
    pss_force_max_amber = models.FloatField(default=70)
    pss_phase_avg_green = models.FloatField(default=3.6)
    pss_phase_avg_amber = models.FloatField(default=4.0)
    pss_phase_max_green = models.FloatField(default=9)
    pss_phase_max_amber = models.FloatField(default=10)
    pss_thd_avg_green = models.FloatField(default=18)
    pss_thd_avg_amber = models.FloatField(default=20)
    pss_thd_max_green = models.FloatField(default=45)
    pss_thd_max_amber = models.FloatField(default=50)

    class Meta:
        ordering = ['name']

    def __str__(self):
        return self.name


class SurveyFile(models.Model):
    FILE_TYPE_CHOICES = [
        ('rx', 'Receiver (Rx)'),
        ('sx', 'Source (Sx)'),
    ]
    DATUM_CHOICES = [
        ('mga2020', 'GDA2020 / MGA2020'),
        ('itrf2014', 'ITRF2014 / UTM South'),
        ('mga94',   'GDA94 / MGA94'),
        ('agd84',   'AGD84 / AMG84'),
        ('agd66',   'AGD66 / AMG66'),
        ('wgs84',   'WGS84 / UTM South'),
    ]
    ZONE_CHOICES = [(str(z), f'Zone {z}') for z in range(46, 59)]

    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='survey_files')
    file_type = models.CharField(max_length=2, choices=FILE_TYPE_CHOICES)
    datum = models.CharField(max_length=10, choices=DATUM_CHOICES)
    zone = models.CharField(max_length=2, choices=ZONE_CHOICES)
    file = models.FileField(upload_to='survey_files/')
    is_final = models.BooleanField(default=False)
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.get_file_type_display()} — {self.get_datum_display()} Zone {self.zone} — {self.job}"


class PersonnelName(models.Model):
    ROLE_CHOICES = [
        ('observer', 'Observer'),
        ('operator', 'Vibe Operator'),
    ]
    name = models.CharField(max_length=100)
    role = models.CharField(max_length=20, choices=ROLE_CHOICES)

    class Meta:
        unique_together = ['name', 'role']
        ordering = ['name']

    def __str__(self):
        return f"{self.name} ({self.get_role_display()})"


class ActivityCategory(models.Model):
    name = models.CharField(max_length=100, unique=True)

    class Meta:
        ordering = ['name']

    def __str__(self):
        return self.name


class ActivityType(models.Model):
    name = models.CharField(max_length=100, unique=True)
    order = models.PositiveIntegerField(default=0)
    chargeable_percentage = models.PositiveSmallIntegerField(default=100)

    class Meta:
        ordering = ['order', 'name']

    def __str__(self):
        return self.name


class Activity(models.Model):
    report = models.ForeignKey('DailyReport', on_delete=models.CASCADE, related_name='activities')
    start_time = models.TimeField()
    end_time = models.TimeField()
    category = models.CharField(max_length=100)
    activity_type = models.CharField(max_length=100, default='Work')
    details = models.CharField(max_length=300, blank=True)
    notes = models.CharField(max_length=300, blank=True)
    job_title = models.CharField(max_length=200, blank=True)
    hours = models.CharField(max_length=20, blank=True)
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['order', 'start_time']

    def __str__(self):
        return f"{self.start_time} — {self.get_category_display()}"

    @property
    def duration(self):
        from datetime import datetime, date
        start = datetime.combine(date.today(), self.start_time)
        end = datetime.combine(date.today(), self.end_time)
        delta = end - start
        total = int(delta.total_seconds() / 60)
        return f"{total // 60:02d}:{total % 60:02d}"


class ReportFile(models.Model):
    FILE_TYPE_CHOICES = [
        ('zip', 'Zip Archive'),
        ('obslog', 'Obs Log'),
        ('pss', 'PSS'),
        ('cog', 'COG'),
        ('rx_deployment', 'Receiver Deployment'),
    ]

    report = models.ForeignKey('DailyReport', on_delete=models.CASCADE, related_name='files')
    file_type = models.CharField(max_length=20, choices=FILE_TYPE_CHOICES)
    file = models.FileField(upload_to='report_files/')
    original_name = models.CharField(max_length=255)
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.get_file_type_display()} — {self.original_name}"


class Skill(models.Model):
    name = models.CharField(max_length=100, unique=True)

    class Meta:
        ordering = ['name']

    def __str__(self):
        return self.name


class Personnel(models.Model):
    name = models.CharField(max_length=100)
    phone = models.CharField(max_length=50, blank=True)
    email = models.EmailField(max_length=200, blank=True)
    skills = models.ManyToManyField(Skill, blank=True, related_name='personnel')

    class Meta:
        ordering = ['name']

    def __str__(self):
        return self.name


class Vehicle(models.Model):
    name = models.CharField(max_length=100)
    vehicle_type = models.CharField(max_length=100, blank=True)
    notes = models.CharField(max_length=300, blank=True)
    description = models.CharField(max_length=200, blank=True)
    category = models.CharField(max_length=100, blank=True)
    make = models.CharField(max_length=100, blank=True)
    model_name = models.CharField(max_length=100, blank=True)
    rego = models.CharField(max_length=30, blank=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        ordering = ['vehicle_type', 'name']

    def __str__(self):
        return self.name


class Equipment(models.Model):
    name = models.CharField(max_length=100)
    serial_number = models.CharField(max_length=100, blank=True)
    equipment_type = models.CharField(max_length=100, blank=True)
    notes = models.CharField(max_length=300, blank=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        ordering = ['equipment_type', 'name']

    def __str__(self):
        return self.name


class JobPersonnel(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='personnel')
    name = models.CharField(max_length=100)
    role = models.CharField(max_length=100, blank=True)
    notes = models.CharField(max_length=300, blank=True)
    sort_order = models.PositiveIntegerField(default=0)
    is_separator = models.BooleanField(default=False)

    class Meta:
        ordering = ['name']

    def __str__(self):
        return f"{self.name} ({self.role})" if self.role else self.name


class JobVehicle(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='vehicles')
    name = models.CharField(max_length=100)
    vehicle_type = models.CharField(max_length=100, blank=True)
    notes = models.CharField(max_length=300, blank=True)

    class Meta:
        ordering = ['vehicle_type', 'name']


class VehicleAllocation(models.Model):
    job_vehicle = models.ForeignKey(JobVehicle, on_delete=models.CASCADE, related_name='allocations')
    person_name = models.CharField(max_length=200)
    tab = models.CharField(max_length=10, default='job')

    class Meta:
        unique_together = ('job_vehicle', 'person_name', 'tab')

    def __str__(self):
        return self.name


class JobEquipment(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='equipment')
    name = models.CharField(max_length=100)
    equipment_type = models.CharField(max_length=100, blank=True)
    notes = models.CharField(max_length=300, blank=True)
    class Meta:
        ordering = ['equipment_type', 'name']

    def __str__(self):
        return self.name


class JobEquipmentVehicleLink(models.Model):
    """Links a JobEquipment item to a JobVehicle within a specific allocation tab."""
    job_equipment = models.ForeignKey(JobEquipment, on_delete=models.CASCADE, related_name='vehicle_links')
    job_vehicle = models.ForeignKey(JobVehicle, on_delete=models.CASCADE, related_name='equipment_links')
    tab = models.CharField(max_length=10, default='job')

    class Meta:
        unique_together = ('job_equipment', 'tab')


class EquipmentAllocation(models.Model):
    job_equipment = models.ForeignKey(JobEquipment, on_delete=models.CASCADE, related_name='allocations')
    person_name = models.CharField(max_length=200)

    class Meta:
        unique_together = ('job_equipment', 'person_name')


class SupervisorActivityTemplate(models.Model):
    name = models.CharField(max_length=200)
    rows_json = models.TextField()  # JSON array of row dicts
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['name']

    @property
    def rows(self):
        import json
        try:
            return json.loads(self.rows_json)
        except Exception:
            return []

    def __str__(self):
        return self.name


class SupervisorOption(models.Model):
    OPTION_TYPE_CHOICES = [
        ('contractor', 'Contractor'),
        ('person', 'Name'),
        ('job_title', 'Job Title'),
    ]
    option_type = models.CharField(max_length=20, choices=OPTION_TYPE_CHOICES)
    name = models.CharField(max_length=200)

    class Meta:
        ordering = ['option_type', 'name']
        unique_together = ['option_type', 'name']

    def __str__(self):
        return f"{self.get_option_type_display()}: {self.name}"


class DiaryTemplate(models.Model):
    name = models.CharField(max_length=100)
    body = models.TextField()
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['order', 'name']

    def __str__(self):
        return self.name


class DailyReport(models.Model):
    STATUS_CHOICES = [
        ('draft', 'Draft'),
        ('submitted', 'Submitted'),
        ('approved', 'Approved'),
    ]

    REPORT_TYPE_CHOICES = [
        ('production', 'Production'),
        ('supervisor', 'Supervisor'),
        ('survey', 'Survey'),
    ]

    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='reports')
    report_type = models.CharField(max_length=20, choices=REPORT_TYPE_CHOICES)
    date = models.DateField()
    observers = models.TextField(blank=True)
    operators = models.TextField(blank=True)
    diary = models.TextField(blank=True)
    supervisor = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='draft')
    include_in_avg = models.BooleanField(default=True)
    include_shot_chart = models.BooleanField(default=False)
    photo_columns = models.IntegerField(default=2)
    last_line_in_ground = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    last_station_in_ground = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-date']

    def __str__(self):
        return f"{self.job} — {self.get_report_type_display()} — {self.date}"


def report_photo_path(instance, filename):
    return f"report_photos/{instance.report.pk}/{filename}"


class ReportPhoto(models.Model):
    BORDER_CHOICES = [
        ('none', 'None'),
        ('thin', 'Thin'),
        ('thick', 'Thick'),
        ('shadow', 'Shadow'),
    ]

    report = models.ForeignKey(DailyReport, on_delete=models.CASCADE, related_name='photos')
    image = models.ImageField(upload_to=report_photo_path)
    caption = models.TextField(blank=True)
    border_style = models.CharField(max_length=10, choices=BORDER_CHOICES, default='none')
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['order', 'pk']

    def __str__(self):
        return f"Photo {self.pk} — Report {self.report_id}"


class JourneyManagementPlan(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='journey_plans')

    plan_number = models.CharField(max_length=50, blank=True)

    def save(self, *args, **kwargs):
        if not self.plan_number:
            from django.utils import timezone
            self.plan_number = timezone.now().strftime('JMP-%Y%m%d-%H%M')
        super().save(*args, **kwargs)

    # Intended journey
    departing_from = models.CharField(max_length=200, blank=True)
    depart_date = models.DateField(null=True, blank=True)
    depart_time = models.TimeField(null=True, blank=True)
    depart_contact = models.CharField(max_length=100, blank=True)
    depart_phone = models.CharField(max_length=50, blank=True)

    # Overnight break
    overnight_location = models.CharField(max_length=200, blank=True)
    overnight_arrival_date = models.DateField(null=True, blank=True)
    overnight_arrival_time = models.TimeField(null=True, blank=True)
    overnight_departure_date = models.DateField(null=True, blank=True)
    overnight_departure_time = models.TimeField(null=True, blank=True)

    # Destination
    arriving_at = models.CharField(max_length=200, blank=True)
    arrive_date = models.DateField(null=True, blank=True)
    arrive_time = models.TimeField(null=True, blank=True)
    arrive_contact = models.CharField(max_length=100, blank=True)
    arrive_phone = models.CharField(max_length=50, blank=True)

    # Route & instructions
    route = models.TextField(blank=True)
    break_journey_at = models.TextField(blank=True)
    radio_channel = models.CharField(max_length=50, blank=True)
    other_instructions = models.TextField(blank=True)
    route_waypoints = models.TextField(blank=True)  # JSON array of {name,lat,lon}
    rest_stops_json = models.TextField(blank=True)  # JSON array of {name,lat,lon}

    # Coordinator
    coordinator_name = models.CharField(max_length=100, blank=True)
    coordinator_phone = models.CharField(max_length=50, blank=True)

    # Before journey
    plan_communicated = models.BooleanField(null=True, blank=True)
    before_signature = models.CharField(max_length=100, blank=True)
    before_date = models.DateField(null=True, blank=True)

    # After journey
    journey_completed = models.BooleanField(null=True, blank=True)
    after_signature = models.CharField(max_length=100, blank=True)
    after_date = models.DateField(null=True, blank=True)

    # Action items
    action_items = models.TextField(blank=True)
    include_map_in_pdf = models.BooleanField(default=True)
    map_tile_layer = models.CharField(max_length=20, default='street')

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"JMP {self.pk} — {self.job} — {self.depart_date or 'no date'}"


class JourneyPersonnel(models.Model):
    plan = models.ForeignKey(JourneyManagementPlan, on_delete=models.CASCADE, related_name='personnel')
    rego = models.CharField(max_length=20, blank=True)
    name = models.CharField(max_length=100, blank=True)
    is_driver = models.BooleanField(default=False)
    phone = models.CharField(max_length=50, blank=True)
    signature = models.CharField(max_length=100, blank=True)
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['name']


class JobPersonnelRole(models.Model):
    job_personnel = models.ForeignKey(JobPersonnel, on_delete=models.CASCADE, related_name='roles')
    role = models.CharField(max_length=100)

    class Meta:
        unique_together = ['job_personnel', 'role']
        ordering = ['role']

    def __str__(self):
        return self.role


class ScheduleStatus(models.Model):
    PATTERN_CHOICES = [
        ('solid', 'Solid'),
        ('stripes', 'Stripes'),
        ('dots', 'Dots'),
        ('cross', 'Cross-hatch'),
    ]
    name = models.CharField(max_length=50, unique=True)
    color = models.CharField(max_length=7, default='#4caf50')
    pattern = models.CharField(max_length=20, choices=PATTERN_CHOICES, default='solid')
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['order', 'name']

    def __str__(self):
        return self.name


class PersonnelScheduleEntry(models.Model):
    job_personnel = models.ForeignKey(JobPersonnel, on_delete=models.CASCADE, related_name='schedule')
    date = models.DateField()
    status = models.CharField(max_length=20, blank=True)  # '' = empty, 'W' = working
    note = models.CharField(max_length=80, blank=True)

    class Meta:
        unique_together = ['job_personnel', 'date']

    def __str__(self):
        return f"{self.job_personnel} — {self.date} — {self.status}"


class JobSkillRequirement(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='skill_requirements')
    skill = models.ForeignKey(Skill, on_delete=models.CASCADE, related_name='job_requirements')
    count = models.PositiveSmallIntegerField(default=0)

    class Meta:
        unique_together = ['job', 'skill']

    def __str__(self):
        return f"{self.job} — {self.skill.name}: {self.count}"


class JobLocation(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='saved_locations')
    name = models.CharField(max_length=200)

    class Meta:
        unique_together = ['job', 'name']
        ordering = ['name']

    def __str__(self):
        return self.name


class JobMusterPoint(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='saved_muster_points')
    name = models.CharField(max_length=200)

    class Meta:
        unique_together = ['job', 'name']
        ordering = ['name']

    def __str__(self):
        return self.name


class ToolboxTopicTemplate(models.Model):
    name = models.CharField(max_length=100)
    body = models.TextField()
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['order', 'name']

    def __str__(self):
        return self.name


class ToolboxMeeting(models.Model):
    TYPE_DAILY = 'daily'
    TYPE_JMP = 'jmp'
    TYPE_CHOICES = [
        (TYPE_DAILY, 'Daily Toolbox Meeting'),
        (TYPE_JMP, 'Journey Management Toolbox'),
    ]
    UV_CHOICES = [('low', 'Low'), ('high', 'High'), ('extreme', 'Extreme')]
    FIRE_CHOICES = [('low', 'Low'), ('high', 'High'), ('extreme', 'Extreme')]

    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='toolbox_meetings')
    meeting_type = models.CharField(max_length=10, choices=TYPE_CHOICES, default=TYPE_DAILY)

    # Section 1
    date = models.DateField(null=True, blank=True)
    time = models.TimeField(null=True, blank=True)
    location = models.CharField(max_length=200, blank=True)
    supervisor = models.CharField(max_length=100, blank=True)
    days_on_job = models.CharField(max_length=20, blank=True)
    jmp_number = models.CharField(max_length=50, blank=True)

    # Section 2
    yesterday_activities = models.TextField(blank=True)
    jmp_route_discussed = models.BooleanField(default=False)
    jmp_hours_noted = models.BooleanField(default=False)
    jmp_contact_numbers = models.BooleanField(default=False)
    jmp_signed_off = models.BooleanField(default=False)
    jmp_lead_tail = models.BooleanField(default=False)

    # Section 3
    todays_activities = models.TextField(blank=True)
    terrain_discussion = models.TextField(blank=True)
    road_condition = models.TextField(blank=True)
    muster_point = models.CharField(max_length=200, blank=True)
    forecast = models.TextField(blank=True)
    uv_index = models.CharField(max_length=10, choices=UV_CHOICES, blank=True)
    chance_of_rain = models.CharField(max_length=50, blank=True)
    min_temp = models.CharField(max_length=20, blank=True)
    max_temp = models.CharField(max_length=20, blank=True)
    grass_fire = models.CharField(max_length=10, choices=FIRE_CHOICES, blank=True)
    forest_fire = models.CharField(max_length=10, choices=FIRE_CHOICES, blank=True)
    wind = models.CharField(max_length=100, blank=True)

    # Section 4
    other_topics = models.TextField(blank=True)
    photo_columns = models.IntegerField(default=2)
    include_equipment_allocation = models.BooleanField(default=False)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-date', '-created_at']

    def __str__(self):
        return f"{self.get_meeting_type_display()} — {self.job} — {self.date or 'no date'}"


def toolbox_photo_path(instance, filename):
    return f"toolbox_photos/{instance.meeting.pk}/{filename}"


class ToolboxPhoto(models.Model):
    BORDER_CHOICES = [('none', 'None'), ('thin', 'Thin'), ('thick', 'Thick'), ('shadow', 'Shadow')]
    meeting = models.ForeignKey('ToolboxMeeting', on_delete=models.CASCADE, related_name='photos')
    image = models.ImageField(upload_to=toolbox_photo_path)
    caption = models.TextField(blank=True)
    border_style = models.CharField(max_length=10, choices=BORDER_CHOICES, default='none')
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['order', 'pk']

    def __str__(self):
        return f"Photo {self.pk} — Toolbox {self.meeting_id}"


class ToolboxAttendee(models.Model):
    meeting = models.ForeignKey(ToolboxMeeting, on_delete=models.CASCADE, related_name='attendees')
    is_separator = models.BooleanField(default=False)
    name = models.CharField(max_length=100, blank=True)
    job_role = models.CharField(max_length=200, blank=True)
    vehicle = models.CharField(max_length=100, blank=True)
    bac = models.CharField(max_length=20, blank=True)
    signature = models.CharField(max_length=100, blank=True)
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['order', 'pk']


class Take5Record(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='take5_records')
    submitted_by = models.CharField(max_length=100)
    submitted_at = models.DateTimeField(auto_now_add=True)
    task_description = models.TextField()
    acknowledged = models.BooleanField(default=False)

    class Meta:
        ordering = ['-submitted_at']

    def __str__(self):
        return f"Take5 — {self.submitted_by} — {self.submitted_at:%Y-%m-%d %H:%M}"


class Take5Hazard(models.Model):
    record = models.ForeignKey(Take5Record, on_delete=models.CASCADE, related_name='hazards')
    hazard = models.CharField(max_length=300)
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['order', 'pk']

    def __str__(self):
        return self.hazard


class Take5Control(models.Model):
    hazard = models.ForeignKey(Take5Hazard, on_delete=models.CASCADE, related_name='controls')
    control = models.CharField(max_length=300)
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['order', 'pk']

    def __str__(self):
        return self.control


# ---------------------------------------------------------------------------
# Toolbox V2 — digital signature version (exact field clone of ToolboxMeeting)
# ---------------------------------------------------------------------------

class ToolboxV2Meeting(models.Model):
    TYPE_DAILY = 'daily'
    TYPE_JMP = 'jmp'
    TYPE_CHOICES = [
        (TYPE_DAILY, 'Daily Toolbox Meeting'),
        (TYPE_JMP, 'Journey Management Toolbox'),
    ]
    UV_CHOICES = [('low', 'Low'), ('high', 'High'), ('extreme', 'Extreme')]
    FIRE_CHOICES = [('low', 'Low'), ('high', 'High'), ('extreme', 'Extreme')]

    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='toolbox_v2_meetings')
    meeting_type = models.CharField(max_length=10, choices=TYPE_CHOICES, default=TYPE_DAILY)

    # Section 1
    date = models.DateField(null=True, blank=True)
    time = models.TimeField(null=True, blank=True)
    location = models.CharField(max_length=200, blank=True)
    supervisor = models.CharField(max_length=100, blank=True)
    days_on_job = models.CharField(max_length=20, blank=True)
    jmp_number = models.CharField(max_length=50, blank=True)

    # Section 2
    yesterday_activities = models.TextField(blank=True)
    jmp_route_discussed = models.BooleanField(default=False)
    jmp_hours_noted = models.BooleanField(default=False)
    jmp_contact_numbers = models.BooleanField(default=False)
    jmp_signed_off = models.BooleanField(default=False)
    jmp_lead_tail = models.BooleanField(default=False)

    # Section 3
    todays_activities = models.TextField(blank=True)
    terrain_discussion = models.TextField(blank=True)
    road_condition = models.TextField(blank=True)
    muster_point = models.CharField(max_length=200, blank=True)
    forecast = models.TextField(blank=True)
    uv_index = models.CharField(max_length=10, choices=UV_CHOICES, blank=True)
    chance_of_rain = models.CharField(max_length=50, blank=True)
    min_temp = models.CharField(max_length=20, blank=True)
    max_temp = models.CharField(max_length=20, blank=True)
    grass_fire = models.CharField(max_length=10, choices=FIRE_CHOICES, blank=True)
    forest_fire = models.CharField(max_length=10, choices=FIRE_CHOICES, blank=True)
    wind = models.CharField(max_length=100, blank=True)

    # Section 4
    other_topics = models.TextField(blank=True)
    photo_columns = models.IntegerField(default=2)
    include_equipment_allocation = models.BooleanField(default=False)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-date', '-created_at']

    def __str__(self):
        return f"Toolbox V2 — {self.job} — {self.date or 'no date'}"


class ToolboxV2Attendee(models.Model):
    meeting = models.ForeignKey(ToolboxV2Meeting, on_delete=models.CASCADE, related_name='attendees')
    is_separator = models.BooleanField(default=False)
    name = models.CharField(max_length=100, blank=True)
    job_role = models.CharField(max_length=200, blank=True)
    vehicle = models.CharField(max_length=100, blank=True)
    bac = models.CharField(max_length=20, blank=True)
    signature = models.TextField(blank=True)  # base64 PNG data URL
    signed_at = models.DateTimeField(null=True, blank=True)
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['order', 'pk']

    def __str__(self):
        return f"{self.name} — Toolbox V2 {self.meeting_id}"


def toolbox_v2_photo_path(instance, filename):
    return f"toolbox_v2_photos/{instance.meeting.pk}/{filename}"


class ToolboxV2Photo(models.Model):
    BORDER_CHOICES = [('none', 'None'), ('thin', 'Thin'), ('thick', 'Thick'), ('shadow', 'Shadow')]
    meeting = models.ForeignKey('ToolboxV2Meeting', on_delete=models.CASCADE, related_name='photos')
    image = models.ImageField(upload_to=toolbox_v2_photo_path)
    caption = models.TextField(blank=True)
    border_style = models.CharField(max_length=10, choices=BORDER_CHOICES, default='none')
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['order', 'pk']

    def __str__(self):
        return f"Photo {self.pk} — Toolbox V2 {self.meeting_id}"


class JourneyV2Plan(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='journey_v2_plans')

    plan_number = models.CharField(max_length=50, blank=True)

    def save(self, *args, **kwargs):
        if not self.plan_number:
            from django.utils import timezone
            self.plan_number = timezone.now().strftime('JMP-%Y%m%d-%H%M')
        super().save(*args, **kwargs)

    # Intended journey
    departing_from = models.CharField(max_length=200, blank=True)
    depart_date = models.DateField(null=True, blank=True)
    depart_time = models.TimeField(null=True, blank=True)
    depart_contact = models.CharField(max_length=100, blank=True)
    depart_phone = models.CharField(max_length=50, blank=True)

    # Overnight break
    overnight_location = models.CharField(max_length=200, blank=True)
    overnight_arrival_date = models.DateField(null=True, blank=True)
    overnight_arrival_time = models.TimeField(null=True, blank=True)
    overnight_departure_date = models.DateField(null=True, blank=True)
    overnight_departure_time = models.TimeField(null=True, blank=True)

    # Destination
    arriving_at = models.CharField(max_length=200, blank=True)
    arrive_date = models.DateField(null=True, blank=True)
    arrive_time = models.TimeField(null=True, blank=True)
    arrive_contact = models.CharField(max_length=100, blank=True)
    arrive_phone = models.CharField(max_length=50, blank=True)

    # Route & instructions
    route = models.TextField(blank=True)
    break_journey_at = models.TextField(blank=True)
    radio_channel = models.CharField(max_length=50, blank=True)
    other_instructions = models.TextField(blank=True)
    route_waypoints = models.TextField(blank=True)
    rest_stops_json = models.TextField(blank=True)

    # Coordinator
    coordinator_name = models.CharField(max_length=100, blank=True)
    coordinator_phone = models.CharField(max_length=50, blank=True)

    # Before journey
    plan_communicated = models.BooleanField(null=True, blank=True)
    before_signature = models.TextField(blank=True)   # base64 PNG
    before_signed_at = models.DateTimeField(null=True, blank=True)
    before_date = models.DateField(null=True, blank=True)

    # After journey
    journey_completed = models.BooleanField(null=True, blank=True)
    after_signature = models.TextField(blank=True)    # base64 PNG
    after_signed_at = models.DateTimeField(null=True, blank=True)
    after_date = models.DateField(null=True, blank=True)

    action_items = models.TextField(blank=True)
    include_map_in_pdf = models.BooleanField(default=True)
    map_tile_layer = models.CharField(max_length=20, default='street')

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"Journey V2 {self.pk} — {self.job} — {self.depart_date or 'no date'}"


class JourneyV2Personnel(models.Model):
    plan = models.ForeignKey(JourneyV2Plan, on_delete=models.CASCADE, related_name='personnel')
    rego = models.CharField(max_length=20, blank=True)
    name = models.CharField(max_length=100, blank=True)
    is_driver = models.BooleanField(default=False)
    phone = models.CharField(max_length=50, blank=True)
    signature = models.TextField(blank=True)   # base64 PNG
    signed_at = models.DateTimeField(null=True, blank=True)
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['name']


class NodeStockTakeSession(models.Model):
    SESSION_TYPES = [('pre_job', 'Pre-Job'), ('post_job', 'Post-Job')]
    job = models.ForeignKey('Job', on_delete=models.CASCADE, related_name='node_stock_takes')
    session_type = models.CharField(max_length=10, choices=SESSION_TYPES, default='pre_job')
    label = models.CharField(max_length=100, blank=True)
    date = models.DateField()
    notes = models.TextField(blank=True)
    nodes_per_crate = models.PositiveIntegerField(default=20)
    crates_per_mega_bin = models.PositiveIntegerField(default=12)
    crate_columns = models.PositiveIntegerField(default=6)
    watch_serials = models.TextField(blank=True)
    watch_color = models.CharField(max_length=20, default='#ff8800')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['date', 'created_at']

    def __str__(self):
        return f"{self.get_session_type_display()} — {self.date}"

    def total_nodes(self):
        return NodeRecord.objects.filter(crate__mega_bin__session=self).count()


class NodeMegaBin(models.Model):
    session = models.ForeignKey(NodeStockTakeSession, on_delete=models.CASCADE, related_name='mega_bins')
    name = models.CharField(max_length=50)
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['order', 'pk']

    def __str__(self):
        return self.name

    def total_nodes(self):
        return NodeRecord.objects.filter(crate__mega_bin=self).count()


class NodeCrate(models.Model):
    mega_bin = models.ForeignKey(NodeMegaBin, on_delete=models.CASCADE, related_name='crates')
    name = models.CharField(max_length=50)
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['order', 'pk']

    def __str__(self):
        return self.name


class NodeRecord(models.Model):
    crate = models.ForeignKey(NodeCrate, on_delete=models.CASCADE, related_name='nodes')
    serial_number = models.CharField(max_length=100)
    slot = models.PositiveIntegerField(default=1)  # 1-based position within crate
    scanned_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['slot']
        unique_together = [('crate', 'slot')]

    def __str__(self):
        return self.serial_number


class DeadNode(models.Model):
    REASON_CHOICES = [
        ('battery', 'Battery Failure'),
        ('physical', 'Physical Damage'),
        ('lost', 'Lost'),
        ('firmware', 'Firmware / Software Issue'),
        ('other', 'Other'),
    ]
    job = models.ForeignKey('Job', on_delete=models.CASCADE, related_name='dead_nodes')
    serial_number = models.CharField(max_length=100)
    reason = models.CharField(max_length=20, choices=REASON_CHOICES)
    notes = models.TextField(blank=True)
    date = models.DateField()
    reported_by = models.CharField(max_length=100, blank=True)

    class Meta:
        ordering = ['-date', 'pk']

    def __str__(self):
        return self.serial_number


def job_photo_path(instance, filename):
    return f"job_photos/{instance.job.pk}/{filename}"


class JobPhoto(models.Model):
    job = models.ForeignKey('Job', on_delete=models.CASCADE, related_name='photos')
    image = models.ImageField(upload_to=job_photo_path)
    caption = models.TextField(blank=True)
    taken_at = models.DateField(null=True, blank=True)
    order = models.PositiveIntegerField(default=0)
    uploaded_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['order', 'pk']

    def __str__(self):
        return f"Photo {self.pk} — Job {self.job_id}"


class PreJobVibFile(models.Model):
    job = models.ForeignKey('Job', on_delete=models.CASCADE, related_name='pre_job_vib_files')
    file = models.FileField(upload_to='pre_job_vib/')
    original_name = models.CharField(max_length=255)
    label = models.CharField(max_length=100, blank=True)
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.label or self.original_name


class TaskSafetyObservation(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='task_observations')
    task_being_observed = models.CharField(max_length=200, blank=True)
    date = models.DateField(null=True, blank=True)
    time = models.TimeField(null=True, blank=True)
    observer = models.CharField(max_length=100, blank=True)
    location = models.CharField(max_length=200, blank=True)
    team_members = models.JSONField(default=list, blank=True)   # list of up to 10 name strings
    checklist = models.JSONField(default=dict, blank=True)      # {0: {answer: 'Y'/'N', comment: ''}, ...}
    at_risk = models.CharField(max_length=3, blank=True)        # 'yes' or 'no'
    discussion = models.TextField(blank=True)
    signature = models.TextField(blank=True)                    # base64 PNG data URL
    signed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-date', '-created_at']

    def __str__(self):
        return f"Task Observation — {self.job} — {self.date or 'no date'}"


class InfieldAudit(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='infield_audits')
    date_commenced = models.DateField(null=True, blank=True)
    date_completed = models.DateField(null=True, blank=True)
    site_location = models.CharField(max_length=200, blank=True)
    audit_conducted_by = models.CharField(max_length=100, blank=True)
    crew_supervisor = models.CharField(max_length=100, blank=True)
    checklist = models.JSONField(default=dict, blank=True)   # {'1a': {answer, comment, date}, ...}
    actions = models.JSONField(default=list, blank=True)     # [{observation, recommendations, priority, responsibility, due_date, completed_by, date}, ...]
    signature = models.TextField(blank=True)
    signed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-date_commenced', '-created_at']

    def __str__(self):
        return f"Infield Audit — {self.job} — {self.date_commenced or 'no date'}"


class JSA(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='jsas')
    # Header
    jsa_name = models.CharField(max_length=300, blank=True)
    reference_tp = models.CharField(max_length=100, blank=True)
    reference_tra = models.CharField(max_length=100, blank=True)
    # Part 1
    project = models.CharField(max_length=200, blank=True)
    site = models.CharField(max_length=200, blank=True)
    date = models.DateField(null=True, blank=True)
    time = models.TimeField(null=True, blank=True)
    job_description = models.TextField(blank=True)
    tools_equipment = models.TextField(blank=True)
    ppe_required = models.TextField(blank=True)
    permits_approvals = models.TextField(blank=True)
    # Part 2 — participants [{name, position, years_exp, signature}]
    participants = models.JSONField(default=list, blank=True)
    # Part 3 — approver
    approver_name = models.CharField(max_length=100, blank=True)
    approver_position = models.CharField(max_length=100, blank=True)
    approver_signature = models.TextField(blank=True)
    approval_date = models.DateField(null=True, blank=True)
    approval_signed_at = models.DateTimeField(null=True, blank=True)
    # Part 4 — analysis [{job_step, hazard, control, person_responsible, managed}]
    analysis = models.JSONField(default=list, blank=True)
    # Part 5 — photos (base64 data URLs)
    photo1 = models.TextField(blank=True)
    photo2 = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-date', '-created_at']

    def __str__(self):
        return f"JSA — {self.job} — {self.jsa_name or 'untitled'}"
