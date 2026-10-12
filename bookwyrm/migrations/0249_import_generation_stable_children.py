# Generated for import parent/child job convergence

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    """Archive digest + import generation on the parent job and
    stable identities on child jobs"""

    dependencies = [
        ("bookwyrm", "0248_update_openlibrary_search_url"),
    ]

    operations = [
        migrations.AddField(
            model_name="bookwyrmimportjob",
            name="archive_hash",
            field=models.CharField(
                blank=True, db_index=True, max_length=64, null=True
            ),
        ),
        migrations.AddField(
            model_name="bookwyrmimportjob",
            name="generation",
            field=models.PositiveIntegerField(default=1),
        ),
        migrations.AddField(
            model_name="bookwyrmimportjob",
            name="source_job",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="retry_jobs",
                to="bookwyrm.bookwyrmimportjob",
            ),
        ),
        migrations.AddField(
            model_name="childjob",
            name="item_key",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddConstraint(
            model_name="childjob",
            constraint=models.UniqueConstraint(
                fields=("parent_job", "item_key"),
                name="unique_childjob_item",
                condition=models.Q(item_key__isnull=False),
            ),
        ),
    ]
