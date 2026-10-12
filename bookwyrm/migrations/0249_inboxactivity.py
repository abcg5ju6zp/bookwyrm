"""idempotent inbox delivery ledger"""

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("bookwyrm", "0248_update_openlibrary_search_url"),
    ]

    operations = [
        migrations.CreateModel(
            name="InboxActivity",
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
                ("activity_id", models.CharField(max_length=255, unique=True)),
                ("sender", models.CharField(db_index=True, max_length=255)),
                ("activity_type", models.CharField(default="", max_length=64)),
                ("body_digest", models.CharField(blank=True, default="", max_length=64)),
                (
                    "signature_key_id",
                    models.CharField(blank=True, default="", max_length=255),
                ),
                ("payload", models.JSONField(blank=True, default=dict)),
                (
                    "ordering_group",
                    models.CharField(blank=True, default="", max_length=700),
                ),
                (
                    "target_remote_id",
                    models.CharField(blank=True, default="", max_length=255),
                ),
                ("generation", models.CharField(blank=True, default="", max_length=40)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("PENDING", "Pending"),
                            ("PROCESSING", "Processing"),
                            ("APPLIED", "Applied"),
                            ("COMPLETED", "Completed"),
                            ("REJECTED", "Rejected"),
                        ],
                        db_index=True,
                        default="PENDING",
                        max_length=32,
                    ),
                ),
                (
                    "rejection_reason",
                    models.CharField(
                        blank=True,
                        choices=[
                            ("stale_generation", "Stale generation"),
                            ("invalid_activity", "Invalid activity"),
                            ("conflict", "Conflicting redelivery"),
                            ("gone_actor", "Sender is gone"),
                        ],
                        default="",
                        max_length=32,
                    ),
                ),
                ("error", models.TextField(blank=True, default="")),
                ("attempts", models.PositiveIntegerField(default=0)),
                ("queued_at", models.DateTimeField(blank=True, null=True)),
                ("resolved_at", models.DateTimeField(blank=True, null=True)),
                ("applied_at", models.DateTimeField(blank=True, null=True)),
                ("completed_at", models.DateTimeField(blank=True, null=True)),
                ("rejected_at", models.DateTimeField(blank=True, null=True)),
                ("created_date", models.DateTimeField(auto_now_add=True)),
                ("updated_date", models.DateTimeField(auto_now=True)),
            ],
            options={},
        ),
        migrations.AddIndex(
            model_name="inboxactivity",
            index=models.Index(
                fields=["ordering_group"], name="bookwyrm_in_orderin_e2d7dc_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="inboxactivity",
            index=models.Index(
                fields=["target_remote_id"], name="bookwyrm_in_target__798b15_idx"
            ),
        ),
    ]
