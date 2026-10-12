"""generation, snapshot, manifest and download leases for user exports"""

import uuid

import django.db.models.deletion
import django.utils.timezone
from django.core.serializers.json import DjangoJSONEncoder
from django.db import migrations, models


def populate_generations(apps, schema_editor):
    """give existing export jobs a unique generation"""
    BookwyrmExportJob = apps.get_model("bookwyrm", "BookwyrmExportJob")
    for job in BookwyrmExportJob.objects.all():
        job.generation = uuid.uuid4()
        job.save(update_fields=["generation"])


class Migration(migrations.Migration):

    dependencies = [
        ("bookwyrm", "0248_update_openlibrary_search_url"),
    ]

    operations = [
        migrations.AddField(
            model_name="bookwyrmexportjob",
            name="generation",
            field=models.UUIDField(
                default=uuid.uuid4, editable=False, null=True, unique=True
            ),
        ),
        migrations.AddField(
            model_name="bookwyrmexportjob",
            name="manifest",
            field=models.JSONField(
                editable=False,
                encoder=DjangoJSONEncoder,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="bookwyrmexportjob",
            name="published_at",
            field=models.DateTimeField(editable=False, null=True),
        ),
        migrations.AddField(
            model_name="bookwyrmexportjob",
            name="snapshot_at",
            field=models.DateTimeField(editable=False, null=True),
        ),
        migrations.RunPython(populate_generations, migrations.RunPython.noop),
        migrations.AlterField(
            model_name="bookwyrmexportjob",
            name="generation",
            field=models.UUIDField(
                default=uuid.uuid4, editable=False, null=False, unique=True
            ),
        ),
        migrations.CreateModel(
            name="ExportDownloadLease",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("created_date", models.DateTimeField(default=django.utils.timezone.now)),
                ("expires_at", models.DateTimeField()),
                (
                    "job",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="download_leases",
                        to="bookwyrm.bookwyrmexportjob",
                    ),
                ),
            ],
            options={
                "indexes": [
                    models.Index(
                        fields=["expires_at"],
                        name="bw_export_lease_expiry_idx",
                    )
                ],
            },
        ),
    ]
