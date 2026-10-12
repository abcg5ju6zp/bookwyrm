"""Import data from Bookwyrm export files"""

import hashlib
import os

from django.core.files import File
from django.core.files.base import ContentFile
from django.db import transaction
from django.db.models import Max
from django.http import QueryDict

from bookwyrm.models import User
from bookwyrm.models.bookwyrm_import_job import BookwyrmImportJob
from . import Importer


def archive_digest(archive_file) -> tuple[object, str]:
    """read the uploaded archive, return (file object ready to save, sha256)"""
    if hasattr(archive_file, "seek"):
        archive_file.seek(0)
        raw = archive_file.read()
        archive_file.seek(0)
        if not hasattr(archive_file, "_committed"):
            # a plain python file object: wrap it for the FileField
            name = (
                os.path.basename(getattr(archive_file, "name", "")) or "archive.tar.gz"
            )
            archive_file = File(archive_file, name=name)
        return archive_file, hashlib.sha256(raw).hexdigest()

    raw = bytes(archive_file)
    return ContentFile(raw, name="archive.tar.gz"), hashlib.sha256(raw).hexdigest()


class BookwyrmImporter:
    """Import a Bookwyrm User export file.
    This is kind of a combination of an importer and a connector.
    """

    def process_import(
        self, user: User, archive_file: bytes, settings: QueryDict
    ) -> BookwyrmImportJob:
        """import user data from a Bookwyrm export file.

        The archive digest identifies the import: re-uploading the same
        archive takes over the in-flight job or returns the completed
        one instead of spawning duplicate tasks. A stopped/failed prior
        upload starts a new generation which only claims failed or
        unconfirmed items."""

        required = [k for k in settings if settings.get(k) == "on"]

        file_obj, digest = archive_digest(archive_file)

        # serialize concurrent uploads for the same user so the
        # (archive_hash, generation) identity can't be duplicated
        with transaction.atomic():
            locked_user = User.objects.select_for_update().get(id=user.id)

            lineage = BookwyrmImportJob.objects.filter(
                user=locked_user, archive_hash=digest
            ).order_by("-generation")
            latest = lineage.first()

            if latest is not None:
                if not latest.complete:
                    # the same archive is already importing: take it over
                    return latest
                if latest.status == BookwyrmImportJob.Status.COMPLETE:
                    # everything already converged for this archive
                    return latest
                # stopped/failed -> fall through to a new generation

            generation = (latest.generation + 1) if latest is not None else 1

            job = BookwyrmImportJob.objects.create(
                user=locked_user,
                archive_file=file_obj,
                required=required,
                archive_hash=digest,
                generation=generation,
                source_job=latest,
            )

        # revoke older, still-running generations outside the lock
        # (revoke makes a network call to the broker)
        job.stop_superseded_jobs()

        return job

    def create_retry_job(
        self, user: User, original_job: BookwyrmImportJob
    ) -> BookwyrmImportJob:
        """retry items that didn't import.

        The retry is the next generation of the same archive: it reuses
        the parsed import data (and its own copy of the stored archive),
        stops any older running generation, and its task manifest only
        claims items that failed or were never confirmed."""

        digest = original_job.archive_hash

        # the retry owns its own stored copy so that completing one job
        # can never delete an archive another job still needs
        archive = original_job.archive_file
        archive_copy = None
        if archive.name:
            archive_copy = ContentFile(
                archive.read(), name=os.path.basename(archive.name)
            )

        with transaction.atomic():
            locked_user = User.objects.select_for_update().get(id=user.id)
            lineage = BookwyrmImportJob.objects.filter(user=locked_user)
            if digest:
                lineage = lineage.filter(archive_hash=digest)
            else:
                lineage = lineage.filter(id=original_job.id)
            generation = (
                lineage.aggregate(max_generation=Max("generation")).get(
                    "max_generation"
                )
                or 1
            ) + 1

            job = BookwyrmImportJob.objects.create(
                user=locked_user,
                archive_file=archive_copy if archive_copy is not None else archive,
                required=original_job.required,
                retry=True,
                archive_hash=digest,
                generation=generation,
                source_job=original_job,
                import_data=original_job.import_data,
            )

        job.stop_superseded_jobs()

        return job


class BookwyrmBooksImporter(Importer):
    """
    Handle reading a csv from BookWyrm.
    Goodreads is the default importer, we basically just use the same structure
    But BookWyrm has additional attributes in the csv
    """

    service = "BookWyrm"
    row_mappings_guesses = Importer.row_mappings_guesses + [
        ("shelf_name", ["shelf_name"]),
        ("review_published", ["review_published"]),
    ]
