"""Export user account to tar.gz file for import into another Bookwyrm instance

Every export has a fixed ``generation`` (a uuid) and a fixed database
``snapshot_at`` cutoff.  The JSON payload and the media files are first written
to a per-generation staging area together with an immutable manifest recording
each file's size and sha256 checksum.  The archive is built and fully verified
in staging, and only then atomically published as the single downloadable
object for that generation.

All of the staging, verification and publication steps are safe to run more
than once for the same generation: on retry (Celery redelivery after an
unclear write result, process restart or the recovery task) existing objects
are checked against the frozen manifest, only missing or corrupt files are
re-created, and the deterministic archive key means a second result is never
published.
"""

import hashlib
import io
import json
import logging
import os
import re
import shutil
import tarfile
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional

from boto3.session import Session as BotoSession
from s3_tar import S3Tar

from django.db import connection, transaction
from django.db.models import (
    CASCADE,
    DateTimeField,
    FileField,
    ForeignKey,
    Index,
    JSONField,
    Model,
    UUIDField,
)
from django.core.serializers.json import DjangoJSONEncoder
from django.core.files.base import ContentFile
from django.core.files.storage import Storage, storages
from django.utils import timezone

from bookwyrm import settings

from bookwyrm.models import AnnualGoal, ReadThrough, ShelfBook, ListItem
from bookwyrm.models import Review, Comment, Quotation
from bookwyrm.models import Edition
from bookwyrm.models import UserFollows, User, UserBlocks
from bookwyrm.models.job import ParentJob, ParentTask
from bookwyrm.tasks import app, IMPORTS, MISC

logger = logging.getLogger(__name__)

#: name of the serialized user data inside the archive
ARCHIVE_MEMBER = "archive.json"
#: name of the immutable manifest inside the archive
MANIFEST_MEMBER = "manifest.json"
#: storage prefix (inside the "exports" storage) holding per-generation staging
STAGING_PREFIX = "staging"
#: jobs that have not reported progress for this long are resumed on restart
RECOVER_STALL = timedelta(minutes=30)
#: a generation uuid used as an archive / staging key
GENERATION_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

CHUNK_SIZE = 5 * 1024 * 1024


class BookwyrmAwsSession(BotoSession):
    """a boto session that always uses settings.AWS_S3_ENDPOINT_URL"""

    def client(self, *args, **kwargs):
        kwargs["endpoint_url"] = storages["exports"].endpoint_url
        return super().client("s3", *args, **kwargs)


def select_exports_storage() -> Storage:
    """callable to allow for dependency on runtime configuration"""
    return storages["exports"]


class BookwyrmExportJob(ParentJob):
    """entry for a specific request to export a bookwyrm user"""

    #: fixed generation of this export; never reused or overwritten
    generation = UUIDField(default=uuid.uuid4, editable=False, unique=True)
    export_data = FileField(null=True, storage=select_exports_storage)
    export_json = JSONField(null=True, encoder=DjangoJSONEncoder)
    #: cutoff for the user data contained in this generation
    snapshot_at = DateTimeField(null=True, editable=False)
    #: immutable list (size + sha256) of every file in this generation
    manifest = JSONField(null=True, encoder=DjangoJSONEncoder, editable=False)
    #: when the archive became the single downloadable object for the job
    published_at = DateTimeField(null=True, editable=False)

    def start_job(self) -> None:
        """schedule the first task"""

        self.set_status("active")
        create_export_json_task.delay(job_id=self.id)

    @property
    def generation_name(self) -> str:
        """the generation as a string"""
        return str(self.generation)

    @property
    def archive_name(self) -> str:
        """immutable, unique name of the published archive for this generation"""
        return f"{self.generation_name}.tar.gz"

    @property
    def staging_prefix(self) -> str:
        """storage prefix of this generation's staging area"""
        return f"{STAGING_PREFIX}/{self.generation_name}/"

    def staging_key(self, path: str) -> str:
        """full key of a staged file"""
        return f"{self.staging_prefix}{path}"

    @property
    def staged_archive_key(self) -> str:
        """key of the verified archive while it is staged in S3"""
        return self.staging_key(self.archive_name)

    @property
    def is_published(self) -> bool:
        """the downloadable object for this generation has been published"""
        return bool(self.complete and self.published_at and self.export_data)

    def json_bytes(self) -> bytes:
        """canonical serialization of the frozen export json"""
        return json_dump(self.export_json).encode("utf-8")


class ExportDownloadLease(Model):
    """an active reference to a published archive, e.g. a download request or
    a pre-signed S3 url that has been handed to the user

    A generation that has an unexpired lease must not be reclaimed by the
    export cleanup tasks.
    """

    job = ForeignKey(
        "BookwyrmExportJob",
        on_delete=CASCADE,
        related_name="download_leases",
    )
    created_date = DateTimeField(default=timezone.now)
    expires_at = DateTimeField()

    class Meta:
        indexes = [
            Index(fields=["expires_at"], name="bw_export_lease_expiry_idx")
        ]

    @classmethod
    def grant(cls, job: BookwyrmExportJob, seconds: Optional[int] = None):
        """record an active download reference for a published generation"""
        seconds = seconds or download_lease_seconds()
        return cls.objects.create(
            job=job, expires_at=timezone.now() + timedelta(seconds=seconds)
        )

    @classmethod
    def purge_expired(cls) -> int:
        """delete leases that can no longer protect anything"""
        deleted, _ = cls.objects.filter(expires_at__lte=timezone.now()).delete()
        return deleted

    @classmethod
    def active_for(cls, job: BookwyrmExportJob) -> bool:
        """whether the generation is currently referenced by a download"""
        return cls.objects.filter(job=job, expires_at__gt=timezone.now()).exists()


def download_lease_seconds() -> int:
    """how long a download reference protects its generation

    For S3 this matches the lifetime of the pre-signed url handed to the
    browser; otherwise it falls back to the configured lease lifetime.
    """
    if settings.USE_S3_FOR_EXPORTS:
        return int(getattr(storages["exports"], "querystring_expire", 0)) or (
            settings.EXPORT_DOWNLOAD_LEASE_SECONDS
        )
    return settings.EXPORT_DOWNLOAD_LEASE_SECONDS


@app.task(queue=IMPORTS, base=ParentTask)
def create_export_json_task(**kwargs) -> None:
    """take the fixed user data snapshot and freeze the immutable manifest"""

    try:
        job = BookwyrmExportJob.objects.get(id=kwargs["job_id"])
        # don't start the job if it was stopped from the UI
        if job.status == "stopped":
            return

        freeze_export_snapshot(job)
        job.refresh_from_db()

        # trigger task to create the tar file. Re-running this task never
        # regenerates the snapshot: the frozen data is reused.
        if not job.is_published:
            create_archive_task.delay(job_id=job.id)

    except BookwyrmExportJob.DoesNotExist:
        # the user (and therefore the job) was deleted while the task queued
        return
    except Exception as err:
        logger.exception(
            "create_export_json_task for job %s failed with error: %s",
            kwargs.get("job_id"),
            err,
        )
        _fail_job(kwargs.get("job_id"))


def _fail_job(job_id) -> None:
    """mark the job failed if it still exists and was never published"""
    if not job_id:
        return
    job = BookwyrmExportJob.objects.filter(id=job_id).first()
    if job and not job.complete and not job.published_at:
        job.set_status("failed")


def freeze_export_snapshot(job: BookwyrmExportJob) -> None:
    """serialize the user data at a fixed point and freeze the manifest

    The json payload and every media byte are staged before the snapshot is
    committed, so the frozen manifest always describes objects that exist in
    the staging area. Nothing about the export is observable until this
    commits.
    """
    job = BookwyrmExportJob.objects.get(id=job.id)
    if job.export_json is not None and job.manifest is not None:
        return

    # claim the snapshot and serialize it from a single read-only database
    # snapshot so concurrent bookshelf/follow/profile edits can't mix in
    snapshot_at = timezone.now()
    data = serialize_user_export(job.user)

    # stage the payload bytes outside of the database transaction
    files = [_stage_bytes(job, ARCHIVE_MEMBER, _encode_json(data), overwrite=True)]

    user = job.user
    if user.avatar:
        with user.avatar.open("rb") as avatar_file:
            files.append(
                _stage_bytes(
                    job, user.avatar.name, avatar_file.read(), overwrite=True
                )
            )

    manifest = {
        "version": 1,
        "generation": job.generation_name,
        "snapshot_at": snapshot_at.isoformat(),
        "files": files,
    }

    with transaction.atomic():
        locked = BookwyrmExportJob.objects.select_for_update().get(id=job.id)
        if locked.status == "stopped":
            return
        # a concurrent worker may already have frozen the snapshot
        if locked.export_json is not None and locked.manifest is not None:
            return
        locked.snapshot_at = snapshot_at
        locked.export_json = data
        locked.manifest = manifest
        locked.save(update_fields=["snapshot_at", "export_json", "manifest"])
        if locked.status == locked.Status.PENDING:
            locked.set_status(locked.Status.ACTIVE)


def serialize_user_export(user: User) -> dict[str, Any]:
    """serialize all user data from a single fixed database snapshot"""
    was_nested = connection.in_atomic_block
    with transaction.atomic():
        if not was_nested:
            # the first statement in the (top level) transaction fixes the
            # postgres snapshot for every query below
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")

        data = export_user(user)
        data["settings"] = export_settings(user)
        data["goals"] = export_goals(user)
        data["books"] = export_books(user)
        data["saved_lists"] = export_saved_lists(user)
        data["follows"] = export_follows(user)
        data["blocks"] = export_blocks(user)
    return data


def _encode_json(data: dict[str, Any]) -> bytes:
    # sort keys so re-encoding the jsonb-roundtripped payload yields exactly
    # the same bytes the frozen manifest records
    return json_dump(data).encode("utf-8")


def json_dump(data: Any) -> str:
    """canonical json encoding (stable key order)"""
    return json.dumps(data, cls=DjangoJSONEncoder, sort_keys=True)


@app.task(queue=IMPORTS, base=ParentTask)
def create_archive_task(**kwargs) -> None:
    """verify the staged files, build the archive and atomically publish it"""

    try:
        job = BookwyrmExportJob.objects.get(id=kwargs["job_id"])
    except BookwyrmExportJob.DoesNotExist:
        return

    # don't start the job if it was stopped from the UI
    if job.status == "stopped":
        return

    try:
        # nothing frozen yet (e.g. the previous worker died mid-snapshot):
        # resume at the snapshot stage instead of publishing a half archive
        if job.export_json is None or job.manifest is None:
            create_export_json_task.delay(job_id=job.id)
            return

        publish_export_archive(job)

    except Exception as err:
        logger.exception(
            "create_archive_task for job %s failed with error: %s", job.id, err
        )
        _fail_job(job.id)


def publish_export_archive(job: BookwyrmExportJob) -> None:
    """reconcile storage with the frozen manifest and publish, idempotently"""
    storage = storages["exports"]

    # fast path: an earlier worker published the object but died before the
    # database state was confirmed. Never build or publish a second archive.
    job.refresh_from_db()
    if job.is_published and storage.exists(job.archive_name):
        _cleanup_staging(job)
        return

    ensure_staged_files(job)
    if settings.USE_S3_FOR_EXPORTS:
        _publish_to_s3(job, storage)
    else:
        _publish_to_local(job, storage)

    discard = False
    with transaction.atomic():
        locked = BookwyrmExportJob.objects.select_for_update().get(id=job.id)
        if locked.status in (locked.Status.FAILED, locked.Status.STOPPED):
            # a concurrent worker gave this generation up: never surface the
            # archive we just built through a stopped/failed job
            discard = True
        elif not locked.is_published:
            locked.export_data = job.archive_name
            locked.published_at = timezone.now()
            locked.save(update_fields=["export_data", "published_at"])
            locked.complete_job()

    if discard:
        reap_export_generation(locked)
    else:
        _cleanup_staging(job)


def ensure_staged_files(job: BookwyrmExportJob) -> None:
    """make sure every file in the manifest is staged intact

    Existing staged objects are verified against the frozen manifest and left
    untouched; only missing or corrupt files are written.
    """
    storage = storages["exports"]
    for entry in job.manifest["files"]:
        key = job.staging_key(entry["path"])
        if storage.exists(key) and _staged_file_matches(storage, key, entry):
            continue
        logger.info("re-staging missing file %s for export %s", key, job.id)
        content = _materialize_manifest_file(job, entry)
        _stage_bytes(job, entry["path"], content, overwrite=True)
        if not _staged_file_matches(storage, key, entry):
            raise ValueError(f"staged file {key} does not match the manifest")


def _materialize_manifest_file(job: BookwyrmExportJob, entry: dict) -> bytes:
    """get the bytes for a manifest file after the snapshot was frozen

    The json can always be regenerated from the frozen payload. Media bytes
    are only taken from storage when they still match the frozen checksum, so
    a profile/avatar change after the export started can never mix into this
    generation.
    """
    path = entry["path"]
    if path == ARCHIVE_MEMBER:
        return job.json_bytes()

    user = job.user
    candidate_storages = []
    if user.avatar and user.avatar.name == path:
        candidate_storages.append(user.avatar.storage)
    if storages["default"] not in candidate_storages:
        candidate_storages.append(storages["default"])

    last_error = None
    for file_storage in candidate_storages:
        try:
            if not file_storage.exists(path):
                continue
            with file_storage.open(path, "rb") as source:
                content = source.read()
        except Exception as err:  # the media object may be gone
            last_error = err
            continue
        if _checksum(content) == entry["checksum"]:
            return content
    raise FileNotFoundError(
        f"media file {path} for export generation {job.generation_name} can no "
        f"longer be reproduced from the frozen snapshot"
        + (f": {last_error}" if last_error else "")
    )


def _publish_to_local(job: BookwyrmExportJob, storage: Storage) -> None:
    """build the archive in the local staging dir and atomically rename it"""
    staged_path = storage.path(job.staged_archive_key)
    published_path = storage.path(job.archive_name)
    os.makedirs(os.path.dirname(staged_path), exist_ok=True)

    with open(staged_path, "wb") as tar_file:
        with tarfile.open(fileobj=tar_file, mode="w:gz") as tar:
            _write_archive_members(job, tar, storage)

    with open(staged_path, "rb") as tar_file:
        _verify_archive(job, tar_file, mode="r:gz")

    # rename within one filesystem is atomic: readers see either the old
    # version or the complete new one, never a partial file
    os.replace(staged_path, published_path)


def _publish_to_s3(job: BookwyrmExportJob, storage: Storage) -> None:
    """build the archive at a staging key and atomically copy it into place"""
    bucket = storage.bucket_name

    # the manifest is part of the archive but not of its own file list
    manifest_bytes = _encode_json(job.manifest)
    _stage_bytes(job, MANIFEST_MEMBER, manifest_bytes, overwrite=True)

    s3_tar = S3Tar(
        bucket,
        _s3_key(storage, job.staged_archive_key),
        session=BookwyrmAwsSession(),
    )
    for entry in job.manifest["files"]:
        s3_tar.add_file(
            _s3_key(storage, job.staging_key(entry["path"])),
            folder=os.path.dirname(entry["path"]),
        )
    s3_tar.add_file(_s3_key(storage, job.staging_key(MANIFEST_MEMBER)))
    s3_tar.tar()

    client = BookwyrmAwsSession().client()
    staged_key = _s3_key(storage, job.staged_archive_key)
    published_key = _s3_key(storage, job.archive_name)

    # verify the complete staged archive before anything becomes visible
    response = client.get_object(Bucket=bucket, Key=staged_key)
    try:
        _verify_archive(job, response["Body"], mode="r|gz")
    finally:
        response["Body"].close()

    # CopyObject is atomic: the published key either stays as the previous
    # object or appears as the complete verified copy
    client.copy_object(
        Bucket=bucket,
        Key=published_key,
        CopySource={"Bucket": bucket, "Key": staged_key},
        ContentType="application/gzip",
        MetadataDirective="REPLACE",
    )
    staged_head = client.head_object(Bucket=bucket, Key=staged_key)
    published_head = client.head_object(Bucket=bucket, Key=published_key)
    if staged_head["ContentLength"] != published_head["ContentLength"]:
        raise ValueError("published S3 object does not match the staged archive")


def _write_archive_members(job: BookwyrmExportJob, tar: tarfile.TarFile, storage) -> None:
    """add every manifest file and the manifest itself to a local tar"""
    for entry in job.manifest["files"]:
        source = storage.open(job.staging_key(entry["path"]), "rb")
        try:
            info = tarfile.TarInfo(name=entry["path"])
            info.size = entry["size"]
            tar.addfile(info, fileobj=source)
        finally:
            source.close()

    info = tarfile.TarInfo(name=MANIFEST_MEMBER)
    manifest_bytes = _encode_json(job.manifest)
    info.size = len(manifest_bytes)
    tar.addfile(info, fileobj=io.BytesIO(manifest_bytes))


def _verify_archive(job: BookwyrmExportJob, fileobj, mode: str) -> None:
    """verify the finished archive against the frozen manifest

    Every listed file must be present with the recorded size and checksum and
    the archive must contain no other members besides the manifest.
    """
    expected = {entry["path"]: entry for entry in job.manifest["files"]}
    seen = set()
    with tarfile.open(fileobj=fileobj, mode=mode) as tar:
        for member in tar:
            if member.name in expected:
                entry = expected[member.name]
                if member.size != entry["size"]:
                    raise ValueError(
                        f"archive member {member.name} has the wrong size"
                    )
                reader = tar.extractfile(member)
                digest = _hash_stream(reader)
                if digest != entry["checksum"]:
                    raise ValueError(
                        f"archive member {member.name} failed checksum verification"
                    )
                seen.add(member.name)
            elif member.name == MANIFEST_MEMBER:
                seen.add(MANIFEST_MEMBER)
            else:
                raise ValueError(f"unexpected archive member {member.name}")

    missing = (set(expected) | {MANIFEST_MEMBER}) - seen
    if missing:
        raise ValueError(f"archive is missing required members: {sorted(missing)}")


# --- storage helpers --------------------------------------------------------


def _stage_bytes(job: BookwyrmExportJob, path: str, content: bytes, overwrite=False):
    """write bytes to a generation's staging area and return a manifest entry"""
    storage = storages["exports"]
    key = job.staging_key(path)

    if settings.USE_S3_FOR_EXPORTS:
        if overwrite and storage.exists(key):
            # the configured S3 storage has file_overwrite disabled
            storage.delete(key)
        storage.save(key, ContentFile(content))
    else:
        full_path = storage.path(key)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "wb") as staged_file:
            staged_file.write(content)

    return {"path": path, "size": len(content), "checksum": _checksum(content)}


def _staged_file_matches(storage: Storage, key: str, entry: dict) -> bool:
    """does the staged object exist and match its manifest entry"""
    try:
        with storage.open(key, "rb") as staged:
            size = 0
            digest = hashlib.sha256()
            for chunk in staged.chunks(CHUNK_SIZE):
                size += len(chunk)
                digest.update(chunk)
    except FileNotFoundError:
        return False
    except Exception as err:
        logger.warning("could not read staged export file %s: %s", key, err)
        return False
    return size == entry["size"] and digest.hexdigest() == _plain_checksum(entry)


def _hash_stream(stream) -> str:
    digest = hashlib.sha256()
    while True:
        chunk = stream.read(CHUNK_SIZE)
        if not chunk:
            break
        digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _checksum(content: bytes) -> str:
    return f"sha256:{hashlib.sha256(content).hexdigest()}"


def _plain_checksum(entry: dict) -> str:
    return entry["checksum"].split(":", 1)[-1]


def _s3_key(storage: Storage, key: str) -> str:
    """prefix a storage-relative key with the storage's S3 location"""
    return os.path.join(storage.location, key) if storage.location else key


def _cleanup_staging(job: BookwyrmExportJob) -> None:
    """best-effort removal of this generation's staged objects"""
    storage = storages["exports"]
    try:
        if settings.USE_S3_FOR_EXPORTS:
            client = BookwyrmAwsSession().client()
            prefix = _s3_key(storage, job.staging_prefix)
            response = client.list_objects_v2(
                Bucket=storage.bucket_name, Prefix=prefix
            )
            keys = [item["Key"] for item in response.get("Contents", [])]
            if keys:
                client.delete_objects(
                    Bucket=storage.bucket_name,
                    Delete={"Objects": [{"Key": key} for key in keys]},
                )
        else:
            _remove_local_dir(storage, job.staging_prefix)
    except Exception as err:
        # stale staging is reclaimed by the orphan sweep, so never fail the job
        logger.warning("could not clean up staging for export %s: %s", job.id, err)


def _remove_local_dir(storage: Storage, relative_path: str) -> None:
    """remove a staged directory and the staging root when it is empty"""
    target = storage.path(relative_path)
    if os.path.isdir(target):
        shutil.rmtree(target)
    if relative_path.startswith(STAGING_PREFIX):
        root = storage.path(STAGING_PREFIX)
        if os.path.isdir(root):
            try:
                os.rmdir(root)
            except OSError:
                # another generation is still staging
                pass


def reap_export_generation(job: BookwyrmExportJob) -> None:
    """delete every storage object belonging to one generation"""
    storage = storages["exports"]
    name = job.export_data.name

    if name:
        try:
            storage.delete(name)
        except Exception as err:
            logger.warning("could not delete export file %s: %s", name, err)

    # also remove a deterministic archive the field may not point at
    if job.archive_name and job.archive_name != name:
        try:
            storage.delete(job.archive_name)
        except Exception as err:
            logger.warning(
                "could not delete export file %s: %s", job.archive_name, err
            )

    if settings.USE_S3_FOR_EXPORTS:
        client = BookwyrmAwsSession().client()
        prefix = _s3_key(storage, job.staging_prefix)
        response = client.list_objects_v2(
            Bucket=storage.bucket_name, Prefix=prefix
        )
        keys = [item["Key"] for item in response.get("Contents", [])]
        if keys:
            client.delete_objects(
                Bucket=storage.bucket_name,
                Delete={"Objects": [{"Key": key} for key in keys]},
            )
    else:
        _remove_local_dir(storage, job.staging_prefix)

    if name:
        # clear the reference on the job now that the object is gone
        job.export_data = ""
        job.save(update_fields=["export_data"])


@app.task(queue=MISC)
def maintain_export_storage_task(**kwargs) -> None:
    """restart recovery and orphan reclamation for user exports

    Runs from the scheduled export cleanup so interrupted exports resume after
    a worker restart and staging/published objects belonging to deleted users
    or abandoned generations are reclaimed.
    """
    ExportDownloadLease.purge_expired()
    recover_interrupted_exports()
    sweep_orphan_export_objects()


def recover_interrupted_exports() -> int:
    """re-enqueue stalled exports so they continue building their generation"""
    cutoff = timezone.now() - RECOVER_STALL
    stalled = (
        BookwyrmExportJob.objects.filter(complete=False)
        .exclude(status__in=["stopped", "failed"])
        .filter(updated_date__lt=cutoff)
    )
    count = 0
    for job in stalled:
        if job.export_json is None:
            create_export_json_task.delay(job_id=job.id)
        else:
            create_archive_task.delay(job_id=job.id)
        count += 1
    if count:
        logger.info("resumed %s interrupted user exports", count)
    return count


def sweep_orphan_export_objects() -> int:
    """delete staged and published objects no generation row accounts for

    Staging for live generations is always left alone. Published objects whose
    user was deleted are kept for the download lease window so a download that
    already started can finish.
    """
    # lazy import to avoid a models package import cycle
    from bookwyrm.models.bookwyrm_import_job import BookwyrmImportJob

    storage = storages["exports"]
    known_generations = set(
        str(g)
        for g in BookwyrmExportJob.objects.values_list("generation", flat=True)
    )
    referenced_names = set(
        BookwyrmExportJob.objects.exclude(export_data="")
        .values_list("export_data", flat=True)
    )
    referenced_names |= set(
        BookwyrmImportJob.objects.exclude(archive_file="")
        .exclude(archive_file__isnull=True)
        .values_list("archive_file", flat=True)
    )

    if settings.USE_S3_FOR_EXPORTS:
        return _sweep_s3(storage, known_generations, referenced_names)
    return _sweep_local(storage, known_generations, referenced_names)


def _sweep_local(storage, known_generations, referenced_names) -> int:
    removed = 0
    grace = timezone.now() - timedelta(seconds=download_lease_seconds())
    root = storage.path("")

    # orphaned published archives at the storage root
    for filename in os.listdir(root):
        full_path = os.path.join(root, filename)
        if not os.path.isfile(full_path) or not filename.endswith(".tar.gz"):
            continue
        generation = filename[: -len(".tar.gz")]
        if not GENERATION_RE.match(generation):
            continue
        if generation in known_generations or filename in referenced_names:
            continue
        modified = datetime.fromtimestamp(
            os.path.getmtime(full_path), tz=timezone.get_current_timezone()
        )
        if modified > grace:
            # still inside the window in which a download may reference it
            continue
        os.remove(full_path)
        removed += 1

    # orphaned staging areas
    staging_root = storage.path(STAGING_PREFIX)
    if os.path.isdir(staging_root):
        for generation in os.listdir(staging_root):
            if not GENERATION_RE.match(generation):
                continue
            if generation in known_generations:
                _remove_terminal_local_staging(
                    storage, os.path.join(staging_root, generation), generation
                )
                continue
            shutil.rmtree(os.path.join(staging_root, generation))
            removed += 1
    return removed


def _remove_terminal_local_staging(storage, path, generation) -> None:
    """drop staging leftovers for terminal generations"""
    job = BookwyrmExportJob.objects.filter(generation=generation).first()
    terminal = job and (
        job.status in ("stopped", "failed")
        or (job.complete and job.published_at)
    )
    if terminal and os.path.isdir(path):
        shutil.rmtree(path)


def _sweep_s3(storage, known_generations, referenced_names) -> int:
    removed = 0
    client = BookwyrmAwsSession().client()
    bucket = storage.bucket_name
    grace = timezone.now() - timedelta(seconds=download_lease_seconds())

    # orphaned published archives at the storage root
    response = client.list_objects_v2(
        Bucket=bucket, Prefix=f"{_s3_key(storage, '')}", Delimiter="/"
    )
    delete_keys = []
    for item in response.get("Contents", []):
        key = item["Key"]
        filename = os.path.basename(key)
        if not filename.endswith(".tar.gz"):
            continue
        generation = filename[: -len(".tar.gz")]
        if not GENERATION_RE.match(generation):
            continue
        if generation in known_generations or filename in referenced_names:
            continue
        if item["LastModified"] > grace:
            continue
        delete_keys.append(key)

    # orphaned staged objects
    staging_prefix = _s3_key(storage, f"{STAGING_PREFIX}/")
    response = client.list_objects_v2(Bucket=bucket, Prefix=staging_prefix)
    staging_generations = set()
    for item in response.get("Contents", []):
        relative = item["Key"][len(staging_prefix) :]
        generation, _, remainder = relative.partition("/")
        if not GENERATION_RE.match(generation) or not remainder:
            continue
        staging_generations.add(generation)
        if generation not in known_generations:
            delete_keys.append(item["Key"])

    for generation in staging_generations & known_generations:
        if _is_terminal_generation(generation):
            prefix = f"{staging_prefix}{generation}/"
            page = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            delete_keys.extend(
                item["Key"] for item in page.get("Contents", [])
            )

    for offset in range(0, len(delete_keys), 1000):
        batch = delete_keys[offset : offset + 1000]
        client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": key} for key in batch]}
        )
        removed += len(batch)

    # interrupted multipart uploads left behind by failed staging runs
    mpu_response = client.list_multipart_uploads(
        Bucket=bucket, Prefix=staging_prefix
    )
    for upload in mpu_response.get("Uploads", []):
        relative = upload["Key"][len(staging_prefix) :]
        generation, _, _ = relative.partition("/")
        if not GENERATION_RE.match(generation):
            continue
        if generation not in known_generations or _is_terminal_generation(
            generation
        ):
            client.abort_multipart_upload(
                Bucket=bucket,
                Key=upload["Key"],
                UploadId=upload["UploadId"],
            )
            removed += 1
    return removed


def _is_terminal_generation(generation: str) -> bool:
    job = BookwyrmExportJob.objects.filter(generation=generation).first()
    return bool(
        job
        and (
            job.status in ("stopped", "failed")
            or (job.complete and job.published_at)
        )
    )


def archive_file_location(file, directory="") -> str:
    """get the relative location of a file inside the archive"""
    return os.path.join(directory, file.name)


def export_user(user: User) -> dict[str, Any]:
    """export user data"""
    data = user.to_activity()
    if user.avatar:
        data["icon"]["url"] = archive_file_location(user.avatar)
    else:
        data["icon"] = {}
    return data


def export_settings(user: User) -> dict[str, bool | str]:
    """Additional settings - can't be serialized as AP"""
    vals = [
        "show_goal",
        "preferred_timezone",
        "default_post_privacy",
        "show_suggested_users",
    ]
    return {k: getattr(user, k) for k in vals}


def export_saved_lists(user: User) -> list[str]:
    """add user saved lists to export JSON"""
    return [saved_list.remote_id for saved_list in user.saved_lists.all()]


def export_follows(user: User) -> list[str]:
    """add user follows to export JSON"""
    follows = UserFollows.objects.filter(user_subject=user).distinct()
    following = User.objects.filter(userfollows_user_object__in=follows).distinct()
    return [f.remote_id for f in following]


def export_blocks(user: User) -> list[str]:
    """add user blocks to export JSON"""
    blocks = UserBlocks.objects.filter(user_subject=user).distinct()
    blocking = User.objects.filter(userblocks_user_object__in=blocks).distinct()
    return [b.remote_id for b in blocking]


def export_goals(user: User) -> list[dict[str, int | str]]:
    """add user reading goals to export JSON"""
    reading_goals = AnnualGoal.objects.filter(user=user).distinct()
    return [
        {"goal": goal.goal, "year": goal.year, "privacy": goal.privacy}
        for goal in reading_goals
    ]


def export_books(user: User) -> list[dict[str, Any]]:
    """add books to export JSON"""
    editions = get_books_for_user(user)
    return [export_book(user, edition) for edition in editions]


def export_book(user: User, edition: Edition) -> dict[str, Any]:
    """add book to export JSON"""
    data = {}
    data["work"] = edition.parent_work.to_activity()
    data["edition"] = edition.to_activity()

    if edition.cover:
        data["edition"]["cover"]["url"] = archive_file_location(
            edition.cover, directory="images"
        )

    # authors
    data["authors"] = [author.to_activity() for author in edition.authors.all()]

    # Shelves this book is on
    # Every ShelfItem is this book so we don't other serializing
    shelf_books = (
        ShelfBook.objects.select_related("shelf")
        .filter(user=user, book=edition)
        .distinct()
    )
    data["shelves"] = [shelfbook.shelf.to_activity() for shelfbook in shelf_books]

    # Lists and ListItems
    # ListItems include "notes" and "approved" so we need them
    # even though we know it's this book
    list_items = ListItem.objects.filter(edition=edition, user=user).distinct()

    data["lists"] = []
    for item in list_items:
        list_info = item.book_list.to_activity()
        list_info["privacy"] = (
            item.book_list.privacy
        )  # this isn't serialized so we add it
        list_info["list_item"] = item.to_activity()
        data["lists"].append(list_info)

    # Statuses
    # Can't use select_subclasses here because
    # we need to filter on the "book" value,
    # which is not available on an ordinary Status
    for status in ["comments", "quotations", "reviews"]:
        data[status] = []

    comments = Comment.objects.filter(user=user, book=edition, deleted=False).all()
    for status in comments:
        obj = status.to_activity()
        obj["progress"] = status.progress
        obj["progress_mode"] = status.progress_mode
        data["comments"].append(obj)

    quotes = Quotation.objects.filter(user=user, book=edition, deleted=False).all()
    for status in quotes:
        obj = status.to_activity()
        obj["position"] = status.position
        obj["endposition"] = status.endposition
        obj["position_mode"] = status.position_mode
        data["quotations"].append(obj)

    reviews = Review.objects.filter(user=user, book=edition, deleted=False).all()
    data["reviews"] = [status.to_activity() for status in reviews]

    # readthroughs can't be serialized to activity
    book_readthroughs = (
        ReadThrough.objects.filter(user=user, book=edition).distinct().values()
    )
    data["readthroughs"] = list(book_readthroughs)
    return data


def get_books_for_user(user: User):
    """
    Get all the books and editions related to a user.
    We use selecting book_id instead of Q objects because it creates
    multiple simple queries instead of a complex DB query
    that can time out.
    """

    shelf_ids = ShelfBook.objects.filter(user=user).values_list("book_id", flat=True)
    readthrough = ReadThrough.objects.filter(user=user).values_list(
        "book_id", flat=True
    )
    reviews = Review.objects.filter(user=user).values_list("book_id", flat=True)
    lists = ListItem.objects.filter(user=user).values_list("edition_id", flat=True)
    comments = Comment.objects.filter(user=user, deleted=False).values_list(
        "book_id", flat=True
    )
    quotes = Quotation.objects.filter(user=user, deleted=False).values_list(
        "book_id", flat=True
    )

    editions = (
        Edition.objects.select_related("parent_work")
        .filter(
            id__in=(
                set(shelf_ids)
                | set(readthrough)
                | set(reviews)
                | set(lists)
                | set(comments)
                | set(quotes)
            )
        )
        .distinct()
    )

    return editions
