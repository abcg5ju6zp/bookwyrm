"""tests for generation-snapshotted, atomically published user exports"""

import datetime
import json
import os
import pathlib
import shutil
import tarfile
import tempfile
import uuid
from unittest.mock import patch

from django.core.files.base import ContentFile
from django.core.files.storage import storages
from django.test import TestCase, RequestFactory
from django.utils import timezone

from bookwyrm import models
from bookwyrm.models.bookwyrm_export_job import (
    create_archive_task,
    create_export_json_task,
    maintain_export_storage_task,
    recover_interrupted_exports,
    reap_export_generation,
    sweep_orphan_export_objects,
)
from bookwyrm.utils.tar import BookwyrmTarFile


class IsolatedExportsStorage(TestCase):
    """point the exports storage at an empty temporary directory per test"""

    def setUp(self):
        super().setUp()
        self.tmpdir = tempfile.mkdtemp()
        self.storage = storages["exports"]
        self._had_location = "location" in self.storage.__dict__
        self._old_location = self.storage.__dict__.get("location")
        self.storage.__dict__["location"] = self.tmpdir

    def tearDown(self):
        if self._had_location:
            self.storage.__dict__["location"] = self._old_location
        else:
            self.storage.__dict__.pop("location", None)
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        super().tearDown()


class ExportPublishingTests(IsolatedExportsStorage):
    """staging, manifest and atomic publication behavior"""

    @classmethod
    def setUpTestData(cls):
        with (
            patch("bookwyrm.suggested_users.rerank_suggestions_task.delay"),
            patch("bookwyrm.activitystreams.populate_stream_task.delay"),
            patch("bookwyrm.lists_stream.populate_lists_task.delay"),
            patch("bookwyrm.models.activitypub_mixin.broadcast_task.apply_async"),
        ):
            cls.user = models.User.objects.create_user(
                "mouse",
                "mouse@mouse.mouse",
                "password",
                local=True,
                localname="mouse",
                name="Mouse",
            )
            avatar_path = pathlib.Path(__file__).parent.joinpath(
                "../../static/images/default_avi.jpg"
            )
            with open(avatar_path, "rb") as avatar_file:
                cls.user.avatar.save("mouse-avatar.jpg", avatar_file)

            cls.user_no_avatar = models.User.objects.create_user(
                "rat", "rat@rat.rat", "password", local=True, localname="rat"
            )

    def _run_json_stage(self, job):
        with patch(
            "bookwyrm.models.bookwyrm_export_job.create_archive_task.delay"
        ):
            create_export_json_task(job_id=job.id)
        job.refresh_from_db()

    def _run_archive_stage(self, job):
        create_archive_task(job_id=job.id)
        job.refresh_from_db()

    def _full_export(self, user=None):
        user = user or self.user
        job = models.BookwyrmExportJob.objects.create(user=user)
        self._run_json_stage(job)
        self._run_archive_stage(job)
        return job

    def _tar_members(self, job):
        with job.export_data.open("rb") as tar_file:
            with tarfile.open(fileobj=tar_file, mode="r:gz") as tar:
                return {m.name: m for m in tar.getmembers()}

    def test_export_has_fixed_generation(self):
        """every export gets a unique, stable generation"""
        first = models.BookwyrmExportJob.objects.create(user=self.user)
        second = models.BookwyrmExportJob.objects.create(user=self.user)
        self.assertIsInstance(first.generation, uuid.UUID)
        self.assertNotEqual(first.generation, second.generation)
        self.assertEqual(first.archive_name, f"{first.generation}.tar.gz")

    def test_snapshot_and_manifest_are_frozen(self):
        """the data cutoff and the immutable manifest are recorded"""
        job = models.BookwyrmExportJob.objects.create(user=self.user)
        self._run_json_stage(job)

        self.assertIsNotNone(job.snapshot_at)
        self.assertEqual(job.manifest["generation"], str(job.generation))
        paths = {entry["path"] for entry in job.manifest["files"]}
        self.assertEqual(paths, {"archive.json", self.user.avatar.name})
        for entry in job.manifest["files"]:
            self.assertGreater(entry["size"], 0)
            self.assertTrue(entry["checksum"].startswith("sha256:"))

    def test_rerun_does_not_mix_in_later_changes(self):
        """re-running the snapshot stage keeps the frozen generation"""
        job = models.BookwyrmExportJob.objects.create(user=self.user)
        self._run_json_stage(job)
        frozen = job.export_json
        manifest = job.manifest

        # profile change after the snapshot was frozen
        self.user.name = "Brand New Mouse"
        self.user.save(broadcast=False, update_fields=["name"])

        self._run_json_stage(job)
        self.assertEqual(job.export_json, frozen)
        self.assertEqual(job.manifest, manifest)
        self.assertEqual(job.export_json["name"], "Mouse")

    def test_unpublished_generation_has_no_download_entry(self):
        """staging state must never expose a file"""
        job = models.BookwyrmExportJob.objects.create(user=self.user)
        self._run_json_stage(job)
        self.assertFalse(job.is_published)
        self.assertFalse(job.export_data.name)

    def test_publish_is_atomic_and_verified(self):
        """only the complete verified archive becomes the download version"""
        job = self._full_export()

        self.assertTrue(job.is_published)
        self.assertEqual(job.export_data.name, job.archive_name)
        self.assertTrue(job.export_data.storage.exists(job.archive_name))

        members = self._tar_members(job)
        self.assertIn("archive.json", members)
        self.assertIn("manifest.json", members)
        self.assertIn(self.user.avatar.name, members)

        # the archive contains exactly the manifest files + manifest.json
        self.assertEqual(
            set(members),
            {"archive.json", "manifest.json", self.user.avatar.name},
        )

        with job.export_data.open("rb") as tar_file, BookwyrmTarFile.open(
            mode="r", fileobj=tar_file
        ) as tar:
            data = json.load(tar.extractfile("archive.json"))
            self.assertEqual(data, job.export_json)
            with self.user.avatar.open() as expected:
                self.assertEqual(
                    expected.read(), tar.extractfile(self.user.avatar.name).read()
                )
            manifest = json.load(tar.extractfile("manifest.json"))
            self.assertEqual(manifest, job.manifest)

    def test_staging_is_cleaned_and_no_second_archive_on_retry(self):
        """re-running the archive stage reconciles to one object"""
        job = self._full_export()
        storage = job.export_data.storage

        staged = storage.path(job.staging_prefix)
        self.assertFalse(os.path.exists(staged))

        published = storage.path(job.archive_name)
        before = os.path.getmtime(published)

        # simulate a redelivered task after an unclear result
        self._run_archive_stage(job)
        self.assertTrue(job.is_published)
        self.assertEqual(os.listdir(storage.path("")), [job.archive_name])
        # fast path did not rebuild a second result
        self.assertEqual(os.path.getmtime(published), before)

    def test_missing_staged_file_is_refilled(self):
        """only the missing staged object is re-created before publishing"""
        job = models.BookwyrmExportJob.objects.create(user=self.user)
        self._run_json_stage(job)

        storage = models.BookwyrmExportJob.export_data.field.storage
        os.remove(storage.path(job.staging_key("archive.json")))

        self._run_archive_stage(job)
        self.assertTrue(job.is_published)
        members = self._tar_members(job)
        self.assertIn("archive.json", members)

    def test_publish_after_db_uncertainty(self):
        """object published but db result lost: reconcile, no second version"""
        job = self._full_export()
        storage = job.export_data.storage
        published_path = storage.path(job.archive_name)
        with open(published_path, "rb") as published:
            expected_bytes = published.read()

        # the storage write landed, the database state did not
        job.export_data = ""
        job.published_at = None
        job.complete = False
        job.status = "active"
        job.save()
        job.refresh_from_db()
        self.assertFalse(job.is_published)

        self._run_archive_stage(job)
        self.assertTrue(job.is_published)
        with open(published_path, "rb") as published:
            self.assertEqual(published.read(), expected_bytes)

    def test_avatar_change_does_not_mix_into_generation(self):
        """a new avatar after the freeze can't replace the staged one"""
        job = models.BookwyrmExportJob.objects.create(user=self.user)
        self._run_json_stage(job)
        frozen_avatar_name = self.user.avatar.name

        new_avatar = pathlib.Path(__file__).parent.joinpath(
            "../../static/images/default_avi.jpg"
        )
        with open(new_avatar, "rb") as avatar_file:
            self.user.avatar.save("totally-new-avatar.jpg", avatar_file)
        self.assertNotEqual(self.user.avatar.name, frozen_avatar_name)

        # drop the staged avatar: it must be restored from the snapshot bytes,
        # not the user's current avatar
        storage = job.export_data.storage
        staged_avatar = storage.path(job.staging_key(frozen_avatar_name))
        os.remove(staged_avatar)

        self._run_archive_stage(job)
        self.assertTrue(job.is_published)
        members = self._tar_members(job)
        self.assertIn(frozen_avatar_name, members)
        self.assertNotIn(self.user.avatar.name, members)

    def test_export_without_media(self):
        """no avatar: manifest and archive contain only the json"""
        job = self._full_export(user=self.user_no_avatar)
        self.assertTrue(job.is_published)
        self.assertEqual(
            [entry["path"] for entry in job.manifest["files"]], ["archive.json"]
        )
        self.assertEqual(
            set(self._tar_members(job)), {"archive.json", "manifest.json"}
        )

    def test_generations_never_overwrite_each_other(self):
        """a newer export neither overwrites nor blocks the older generation"""
        old = self._full_export()
        new = self._full_export()
        storage = old.export_data.storage

        self.assertNotEqual(old.archive_name, new.archive_name)
        self.assertTrue(storage.exists(old.archive_name))
        self.assertTrue(storage.exists(new.archive_name))

    def test_s3_publish_copies_a_verified_staged_object(self):
        """S3 archives are built in staging and atomically copied into place"""
        import io
        from types import SimpleNamespace

        from bookwyrm.models import bookwyrm_export_job as export_job

        job = models.BookwyrmExportJob.objects.create(user=self.user)
        self._run_json_stage(job)

        # build what a real staging key would contain: a complete archive
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
            for entry in job.manifest["files"]:
                source = job.export_data.storage.open(
                    job.staging_key(entry["path"]), "rb"
                )
                try:
                    info = tarfile.TarInfo(name=entry["path"])
                    info.size = entry["size"]
                    archive.addfile(info, fileobj=source)
                finally:
                    source.close()
            manifest_bytes = json.dumps(job.manifest).encode("utf-8")
            info = tarfile.TarInfo(name="manifest.json")
            info.size = len(manifest_bytes)
            archive.addfile(info, fileobj=io.BytesIO(manifest_bytes))
        archive_bytes = archive_buffer.getvalue()

        mock_storage = SimpleNamespace(
            bucket_name="test-bucket",
            location="exports",
            save=lambda *args, **kwargs: None,
            delete=lambda *args, **kwargs: None,
            exists=lambda key: False,
        )
        copied = []
        mock_client = SimpleNamespace(
            get_object=lambda Bucket, Key: {"Body": io.BytesIO(archive_bytes)},
            head_object=lambda Bucket, Key: {
                "ContentLength": len(archive_bytes)
            },
            copy_object=lambda Bucket, Key, CopySource, **kwargs: copied.append(
                (Key, CopySource["Key"])
            ),
        )

        class FakeS3Tar:
            instances = []

            def __init__(self, bucket, target_key, session=None):
                self.bucket = bucket
                self.target_key = target_key
                self.members = []
                FakeS3Tar.instances.append(self)

            def add_file(self, key, folder=""):
                self.members.append((key, folder))

            def tar(self):
                return None

        with (
            patch.object(export_job.settings, "USE_S3_FOR_EXPORTS", True),
            patch.object(export_job, "storages", {"exports": mock_storage}),
            patch.object(export_job, "S3Tar", FakeS3Tar),
            patch.object(
                export_job.BookwyrmAwsSession,
                "client",
                lambda self, *a, **k: mock_client,
            ),
        ):
            export_job._publish_to_s3(job, mock_storage)

        # the archive was built at a staging key and copied exactly once
        self.assertEqual(len(FakeS3Tar.instances), 1)
        self.assertIn(
            f"exports/staging/{job.generation}/{job.archive_name}",
            FakeS3Tar.instances[0].target_key,
        )
        self.assertEqual(
            copied,
            [
                (
                    f"exports/{job.archive_name}",
                    f"exports/staging/{job.generation}/{job.archive_name}",
                )
            ],
        )
        # every manifest file plus the manifest itself was added
        member_keys = {key for key, _ in FakeS3Tar.instances[0].members}
        self.assertIn(
            f"exports/staging/{job.generation}/archive.json", member_keys
        )
        self.assertIn(
            f"exports/staging/{job.generation}/manifest.json", member_keys
        )

    def test_s3_publish_refuses_to_copy_broken_staged_archive(self):
        """verification failure in staging never reaches the published key"""
        import io
        from types import SimpleNamespace

        from bookwyrm.models import bookwyrm_export_job as export_job

        job = models.BookwyrmExportJob.objects.create(user=self.user)
        self._run_json_stage(job)

        mock_storage = SimpleNamespace(
            bucket_name="test-bucket",
            location="exports",
            save=lambda *args, **kwargs: None,
            delete=lambda *args, **kwargs: None,
            exists=lambda key: False,
        )
        copies = []
        mock_client = SimpleNamespace(
            get_object=lambda Bucket, Key: {"Body": io.BytesIO(b"not a tar")},
            head_object=lambda Bucket, Key: {"ContentLength": 9},
            copy_object=lambda Bucket, Key, CopySource, **k: copies.append(Key),
        )

        class FakeS3Tar:
            def __init__(self, *a, **k):
                return None

            def add_file(self, *a, **k):
                return None

            def tar(self):
                return None

        with (
            patch.object(export_job.settings, "USE_S3_FOR_EXPORTS", True),
            patch.object(export_job, "storages", {"exports": mock_storage}),
            patch.object(export_job, "S3Tar", FakeS3Tar),
            patch.object(
                export_job.BookwyrmAwsSession,
                "client",
                lambda self, *a, **k: mock_client,
            ),
            self.assertRaises(Exception),
        ):
            export_job._publish_to_s3(job, mock_storage)
        self.assertEqual(copies, [])


class ExportRecoveryTests(IsolatedExportsStorage):
    """restart resumption and orphan reclamation"""

    @classmethod
    def setUpTestData(cls):
        with (
            patch("bookwyrm.suggested_users.rerank_suggestions_task.delay"),
            patch("bookwyrm.activitystreams.populate_stream_task.delay"),
            patch("bookwyrm.models.activitypub_mixin.broadcast_task.apply_async"),
        ):
            cls.user = models.User.objects.create_user(
                "mouse",
                "mouse@mouse.mouse",
                "password",
                local=True,
                localname="mouse",
            )

    def test_recover_stalled_unfrozen_job(self):
        """a worker dying before the snapshot resumes at the json stage"""
        job = models.BookwyrmExportJob.objects.create(user=self.user)
        job.status = "active"
        job.updated_date = timezone.now() - datetime.timedelta(hours=1)
        job.save(update_fields=["status", "updated_date"])

        with patch(
            "bookwyrm.models.bookwyrm_export_job.create_export_json_task.delay"
        ) as json_task, patch(
            "bookwyrm.models.bookwyrm_export_job.create_archive_task.delay"
        ) as archive_task:
            self.assertEqual(recover_interrupted_exports(), 1)
            json_task.assert_called_once_with(job_id=job.id)
            archive_task.assert_not_called()

    def test_recover_stalled_staging_job(self):
        """a worker dying while staging resumes at the archive stage"""
        job = models.BookwyrmExportJob.objects.create(user=self.user)
        job.export_json = {"name": "Mouse"}
        job.manifest = {"files": []}
        job.status = "active"
        job.updated_date = timezone.now() - datetime.timedelta(hours=1)
        job.save()

        with patch(
            "bookwyrm.models.bookwyrm_export_job.create_export_json_task.delay"
        ) as json_task, patch(
            "bookwyrm.models.bookwyrm_export_job.create_archive_task.delay"
        ) as archive_task:
            self.assertEqual(recover_interrupted_exports(), 1)
            archive_task.assert_called_once_with(job_id=job.id)
            json_task.assert_not_called()

    def test_recent_jobs_are_not_resumed(self):
        """live tasks are not duplicated"""
        job = models.BookwyrmExportJob.objects.create(user=self.user)
        job.status = "active"
        job.save(update_fields=["status"])
        with patch(
            "bookwyrm.models.bookwyrm_export_job.create_export_json_task.delay"
        ) as task:
            self.assertEqual(recover_interrupted_exports(), 0)
            task.assert_not_called()

    def test_terminal_jobs_are_not_resumed(self):
        """failed or stopped generations stay dead"""
        for status in ["failed", "stopped"]:
            job = models.BookwyrmExportJob.objects.create(user=self.user)
            job.status = status
            job.complete = True
            job.updated_date = timezone.now() - datetime.timedelta(hours=1)
            job.save()
        with patch(
            "bookwyrm.models.bookwyrm_export_job.create_export_json_task.delay"
        ) as task:
            self.assertEqual(recover_interrupted_exports(), 0)
            task.assert_not_called()

    def test_maintenance_task_runs_all_steps(self):
        with patch(
            "bookwyrm.models.bookwyrm_export_job.recover_interrupted_exports"
        ) as recover, patch(
            "bookwyrm.models.bookwyrm_export_job.sweep_orphan_export_objects"
        ) as sweep, patch(
            "bookwyrm.models.bookwyrm_export_job.ExportDownloadLease.purge_expired"
        ) as purge:
            maintain_export_storage_task()
            recover.assert_called_once()
            sweep.assert_called_once()
            purge.assert_called_once()


class ExportCleanupTests(IsolatedExportsStorage):
    """leases protect generations and orphans are reclaimed"""

    @classmethod
    def setUpTestData(cls):
        with (
            patch("bookwyrm.suggested_users.rerank_suggestions_task.delay"),
            patch("bookwyrm.activitystreams.populate_stream_task.delay"),
            patch("bookwyrm.models.activitypub_mixin.broadcast_task.apply_async"),
        ):
            cls.user = models.User.objects.create_user(
                "mouse",
                "mouse@mouse.mouse",
                "password",
                local=True,
                localname="mouse",
            )

    def _published_job(self):
        job = models.BookwyrmExportJob.objects.create(user=self.user)
        job.export_data = ContentFile(b"archive-bytes", name=job.archive_name)
        job.save()
        return job

    def test_active_lease_protects_generation(self):
        from bookwyrm.models.housekeeping import delete_user_export_file_task

        job = self._published_job()
        models.ExportDownloadLease.grant(job, seconds=900)
        storage = job.export_data.storage

        self.assertTrue(models.ExportDownloadLease.active_for(job))
        delete_user_export_file_task(export_id=job.id)
        # the object and the reference are both kept
        self.assertTrue(storage.exists(job.archive_name))
        job.refresh_from_db()
        self.assertTrue(job.export_data.name)

    def test_expired_lease_allows_reaping(self):
        from bookwyrm.models.housekeeping import delete_user_export_file_task

        job = self._published_job()
        lease = models.ExportDownloadLease.grant(job, seconds=900)
        lease.expires_at = timezone.now() - datetime.timedelta(seconds=1)
        lease.save(update_fields=["expires_at"])
        storage = job.export_data.storage

        self.assertFalse(models.ExportDownloadLease.active_for(job))
        delete_user_export_file_task(export_id=job.id)
        self.assertFalse(storage.exists(job.archive_name))
        job.refresh_from_db()
        self.assertFalse(job.export_data.name)

    def test_reap_removes_staging_too(self):
        job = models.BookwyrmExportJob.objects.create(user=self.user)
        storage = job.export_data.storage
        staged = storage.path(job.staging_key("archive.json"))
        os.makedirs(os.path.dirname(staged), exist_ok=True)
        with open(staged, "wb") as staged_file:
            staged_file.write(b"{}")

        reap_export_generation(job)
        self.assertFalse(os.path.exists(storage.path(job.staging_prefix)))

    def test_orphan_sweep(self):
        """root and staging objects without a generation row are reclaimed"""
        storage = self._published_job().export_data.storage

        orphan_generation = uuid.uuid4()
        orphan_archive = storage.path(f"{orphan_generation}.tar.gz")
        with open(orphan_archive, "wb") as output:
            output.write(b"old")
        old = timezone.now() - datetime.timedelta(hours=2)
        os.utime(orphan_archive, (old.timestamp(), old.timestamp()))

        orphan_staging = storage.path(
            f"staging/{orphan_generation}/archive.json"
        )
        os.makedirs(os.path.dirname(orphan_staging), exist_ok=True)
        with open(orphan_staging, "wb") as output:
            output.write(b"{}")

        # a fresh orphan (download window still open) and a non-uuid import
        # file must be left alone
        fresh_orphan = storage.path(f"{uuid.uuid4()}.tar.gz")
        with open(fresh_orphan, "wb") as output:
            output.write(b"new")
        import_file = storage.path("user-uploaded-export.tar.gz")
        with open(import_file, "wb") as output:
            output.write(b"import")

        # staging for a live generation is protected
        live_job = models.BookwyrmExportJob.objects.create(user=self.user)
        live_staged = storage.path(live_job.staging_key("archive.json"))
        os.makedirs(os.path.dirname(live_staged), exist_ok=True)
        with open(live_staged, "wb") as output:
            output.write(b"{}")

        removed = sweep_orphan_export_objects()
        self.assertGreaterEqual(removed, 2)
        self.assertFalse(os.path.exists(orphan_archive))
        self.assertFalse(
            os.path.exists(storage.path(f"staging/{orphan_generation}"))
        )
        self.assertTrue(os.path.exists(fresh_orphan))
        self.assertTrue(os.path.exists(import_file))
        self.assertTrue(os.path.exists(live_staged))

    def test_orphan_sweep_keeps_published_generation(self):
        job = self._published_job()
        sweep_orphan_export_objects()
        self.assertTrue(job.export_data.storage.exists(job.archive_name))


class ExportDownloadViewTests(IsolatedExportsStorage):
    """the download endpoint converges on the published generation"""

    @classmethod
    def setUpTestData(cls):
        with (
            patch("bookwyrm.suggested_users.rerank_suggestions_task.delay"),
            patch("bookwyrm.activitystreams.populate_stream_task.delay"),
            patch("bookwyrm.models.activitypub_mixin.broadcast_task.apply_async"),
        ):
            cls.user = models.User.objects.create_user(
                "mouse",
                "mouse@mouse.mouse",
                "password",
                local=True,
                localname="mouse",
            )

    def setUp(self):
        super().setUp()
        self.factory = RequestFactory()

    def test_staging_job_returns_404(self):
        from bookwyrm import views
        from django.http import Http404

        job = models.BookwyrmExportJob.objects.create(user=self.user)
        job.task_id = uuid.uuid4()
        job.save(update_fields=["task_id"])

        request = self.factory.get("")
        request.user = self.user
        with self.assertRaises(Http404):
            views.ExportArchive.as_view()(request, archive_id=job.task_id)

    def test_published_job_downloads_and_leases(self):
        from bookwyrm import views

        job = models.BookwyrmExportJob.objects.create(user=self.user)
        job.export_data = ContentFile(b"archive-bytes", name=job.archive_name)
        job.published_at = timezone.now()
        job.status = "complete"
        job.complete = True
        job.task_id = uuid.uuid4()
        job.save()

        request = self.factory.get("")
        request.user = self.user
        response = views.ExportArchive.as_view()(request, archive_id=job.task_id)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(b"".join(response), b"archive-bytes")
        self.assertTrue(models.ExportDownloadLease.active_for(job))

    def test_list_only_links_published_generations(self):
        from bookwyrm import views

        # staging generation
        staging = models.BookwyrmExportJob.objects.create(user=self.user)
        staging.export_json = {"name": "Mouse"}
        staging.manifest = {"files": []}
        staging.status = "active"
        staging.save()

        # published generation
        published = models.BookwyrmExportJob.objects.create(user=self.user)
        published.export_data = ContentFile(
            b"archive-bytes", name=published.archive_name
        )
        published.published_at = timezone.now()
        published.status = "complete"
        published.complete = True
        published.task_id = uuid.uuid4()
        published.save()

        request = self.factory.get("")
        request.user = self.user
        response = views.ExportUser.as_view()(request)
        jobs = list(response.context_data["jobs"].object_list)
        by_state = {bool(job.get("url")): job["job"].id for job in jobs}
        self.assertEqual(by_state[True], published.id)
        self.assertEqual(by_state[False], staging.id)
