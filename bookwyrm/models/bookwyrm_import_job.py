"""Import a user from another Bookwyrm instance"""

import datetime
import json
import logging
import math
from typing import Any
from urllib.parse import urlparse

from botocore.exceptions import EndpointConnectionError
import requests

from django.apps import apps
from django.db import transaction
from django.db.models import (
    BooleanField,
    CharField,
    ForeignKey,
    FileField,
    JSONField,
    PositiveIntegerField,
    QuerySet,
    TextChoices,
    Q,
    PROTECT,
    SET_NULL,
)
from django.core.files.storage import storages, Storage
from django.utils import timezone
from django.utils.html import strip_tags
from django.utils.translation import gettext_lazy as _
from django.contrib.postgres.fields import ArrayField as DjangoArrayField

from bookwyrm import activitypub, models, settings
from bookwyrm.connectors import connector_manager
from bookwyrm.tasks import app, IMPORTS, MISC
from bookwyrm.models.job import Job, ParentJob, ChildJob, ParentTask, SubTask
from bookwyrm.utils.tar import BookwyrmTarFile

logger = logging.getLogger(__name__)

# a child task which has stayed ACTIVE for this long is presumed dead
# (worker crash or lost completion callback) and can be claimed again
STALE_TASK_LIMIT = datetime.timedelta(hours=1)


def select_exports_storage() -> Storage:
    """callable to allow for dependency on runtime configuration"""
    return storages["exports"]


class BookwyrmImportJob(ParentJob):
    """entry for a specific request for importing a bookwyrm user backup"""

    archive_file = FileField(null=True, blank=True, storage=select_exports_storage)
    import_data = JSONField(null=True)
    required = DjangoArrayField(
        models.fields.CharField(max_length=50, blank=True), blank=True
    )
    retry = BooleanField(default=False)
    # sha256 digest of the uploaded archive. Together with the generation
    # it identifies the import lineage: re-uploading the same archive does
    # not create a competing set of tasks.
    archive_hash = CharField(max_length=64, null=True, blank=True, db_index=True)
    generation = PositiveIntegerField(default=1)
    source_job = ForeignKey(
        "self",
        null=True,
        blank=True,
        on_delete=SET_NULL,
        related_name="retry_jobs",
    )

    # uniqueness of (user, archive_hash, generation) is enforced when
    # jobs are created (see BookwyrmImporter) by locking the user row;
    # a database constraint cannot span the parent table (user_id) and
    # this child table.

    def start_job(self) -> None:
        """Start the job. Does nothing if the job is already running
        (for example when an uploaded archive is taken over)"""

        if self.complete or self.status != self.Status.PENDING:
            return
        start_import_task.delay(job_id=self.id)

    @property
    def lineage(self) -> QuerySet["BookwyrmImportJob"]:
        """all jobs created from the same archive for this user,
        across generations (retries and re-uploads)"""
        if not self.archive_hash:
            return BookwyrmImportJob.objects.filter(id=self.id)
        return BookwyrmImportJob.objects.filter(
            user=self.user, archive_hash=self.archive_hash
        )

    @property
    def is_superseded(self) -> bool:
        """has a newer generation of this archive been started?"""
        return self.lineage.filter(generation__gt=self.generation).exists()

    @property
    def is_live(self) -> bool:
        """may tasks for this job still write data?

        Stops old generations after a re-upload, stopped jobs and tasks
        belonging to deactivated users from overwriting later changes.
        """
        return (
            not self.complete
            and self.status != self.Status.STOPPED
            and self.user.is_active
            and not self.is_superseded
        )

    def lineage_child(self, model, **filters) -> QuerySet:
        """child tasks of any generation of this archive"""
        children = model.objects.filter(
            parent_job__user=self.user,
            **filters,
        )
        if self.archive_hash:
            # parent_job points at the ParentJob base table; archive_hash
            # lives on the BookwyrmImportJob inheritance child
            children = children.filter(
                parent_job__bookwyrmimportjob__archive_hash=self.archive_hash
            )
        else:
            children = children.filter(parent_job=self)
        return children

    def stop_superseded_jobs(self) -> None:
        """revoke any older, still-running generation of this archive"""
        for older in self.lineage.exclude(id=self.id).filter(complete=False):
            older.stop_job()

    @property
    def book_tasks(self) -> QuerySet["UserImportBook"]:
        """How many import book tasks are there?"""
        return UserImportBook.objects.filter(parent_job=self).all()

    @property
    def status_tasks(self) -> QuerySet["UserImportPost"]:
        """How many import status tasks are there?"""
        return UserImportPost.objects.filter(parent_job=self).all()

    @property
    def relationship_tasks(self) -> QuerySet["UserImportRelationship"]:
        """How many import relationship tasks are there?"""
        return UserImportRelationship.objects.filter(parent_job=self).all()

    @property
    def item_count(self) -> int:
        """How many total tasks are there?"""
        return (
            self.book_tasks.count()
            + self.status_tasks.count()
            + self.relationship_tasks.count()
        )

    @property
    def pending_item_count(self) -> int:
        """How many tasks are incomplete?"""
        status = BookwyrmImportJob.Status
        book_tasks = self.book_tasks.filter(
            status__in=[status.PENDING, status.ACTIVE]
        ).count()

        status_tasks = self.status_tasks.filter(
            status__in=[status.PENDING, status.ACTIVE]
        ).count()

        relationship_tasks = self.relationship_tasks.filter(
            status__in=[status.PENDING, status.ACTIVE]
        ).count()

        return book_tasks + status_tasks + relationship_tasks

    @property
    def percent_complete(self) -> int:
        """How far along?"""
        item_count = self.item_count
        if not item_count:
            return 0
        return math.floor((item_count - self.pending_item_count) / item_count * 100)

    def archive_is_referenced(self) -> bool:
        """is another job still pointing at the same stored archive?"""
        if not self.archive_file.name:
            return False
        return BookwyrmImportJob.objects.exclude(id=self.id).filter(
            archive_file=self.archive_file.name
        ).exists()

    def delete_archive_if_unreferenced(self) -> None:
        """reclaim the archive storage once no job references it"""
        if self.archive_file.name and not self.archive_is_referenced():
            self.archive_file.delete(save=True)

    def complete_job(self) -> None:
        """Report that the job has completed and stop pending children."""

        super().complete_job()

        # only delete the import file when no retry job references it
        self.delete_archive_if_unreferenced()

    def notify_child_job_complete(self) -> None:
        """let the job know when the items get work done"""

        if self.complete:
            return

        self.updated_date = timezone.now()
        self.save(update_fields=["updated_date"])

        if not self.complete and self.has_completed:
            self.complete_job()

    def evaluate_completion(self) -> None:
        """Reconcile the parent job from the persisted child results
        rather than from a single completion callback. Safe to call after
        a restart or when a completion notification was lost."""

        if self.complete:
            return

        self.updated_date = timezone.now()
        self.save(update_fields=["updated_date"])

        if self.has_completed:
            self.complete_job()


class UserImportBook(ChildJob):
    """ChildJob to import each book.
    Equivalent to ImportItem when importing a csv file of books"""

    book = ForeignKey(models.Book, on_delete=SET_NULL, null=True, blank=True)
    book_data = JSONField(null=False)

    def start_job(self, origin_is_ok: bool = False) -> None:
        """Start the job"""
        import_book_task.delay(
            child_id=self.id, origin_is_ok=origin_is_ok, job_type="UserImportBook"
        )

    def complete_job(self) -> None:
        """Report to BookwyrmImportJob that the job has completed.
        Do not use super() here because the parent class will
        complete the job over the top of us and then you will be sad."""

        Job.complete_job(self)  # don't notify ParentJob
        parent = BookwyrmImportJob.objects.get(id=self.parent_job.id)
        parent.notify_child_job_complete()


class UserImportPost(ChildJob):
    """ChildJob for comments, quotes, and reviews"""

    class StatusType(TextChoices):
        """Possible status types."""

        COMMENT = "comment", _("Comment")
        REVIEW = "review", _("Review")
        QUOTE = "quote", _("Quotation")

    json = JSONField(null=False)
    book = models.fields.ForeignKey(
        "Edition", on_delete=PROTECT, activitypub_field="inReplyToBook"
    )
    status_type = models.fields.CharField(
        max_length=10, choices=StatusType.choices, default=StatusType.COMMENT, null=True
    )

    def start_job(self) -> None:
        """Start the job"""
        upsert_status_task.delay(child_id=self.id, job_type="UserImportPost")

    def complete_job(self) -> None:
        """Report to BookwyrmImportJob that the job has completed."""

        Job.complete_job(self)  # don't notify ParentJob
        parent = BookwyrmImportJob.objects.get(id=self.parent_job.id)
        parent.notify_child_job_complete()


class UserImportRelationship(ChildJob):
    """ChildJob for follows and blocks"""

    class RelationshipType(TextChoices):
        """Possible relationship types."""

        FOLLOW = "follow", _("Follow")
        BLOCK = "block", _("Block")

    relationship = models.fields.CharField(
        max_length=10, choices=RelationshipType.choices, null=True
    )
    remote_id = models.fields.RemoteIdField(null=True, unique=False)

    def start_job(self) -> None:
        """Start the job"""
        import_user_relationship_task.delay(
            child_id=self.id, job_type="UserImportRelationship"
        )

    def complete_job(self) -> None:
        """Report to BookwyrmImportJob that the job has completed."""

        Job.complete_job(self)  # don't notify ParentJob
        parent = BookwyrmImportJob.objects.get(id=self.parent_job.id)
        parent.notify_child_job_complete()


class ImportUserTask(ParentTask):
    """A task for a user import job"""

    def before_start(self, task_id: str, args, kwargs) -> None:
        """Handler called before the task starts."""
        job = BookwyrmImportJob.objects.get(id=kwargs["job_id"])
        job.task_id = task_id
        job.save(update_fields=["task_id"])


class UserImportSubTask(SubTask):
    """Makes sure we refer to the correct child job and call
    subclass methods instead of methods on ChildJob & ParentJob"""

    def before_start(self, task_id: str, args, kwargs) -> None:
        """Handler called before the task starts."""

        model = apps.get_model(f"bookwyrm.{kwargs['job_type']}", require_ready=True)
        child_job = model.objects.get(id=kwargs["child_id"])
        child_job.task_id = task_id
        child_job.save(update_fields=["task_id"])
        child_job.set_status(ChildJob.Status.ACTIVE)

    def on_success(self, retval, task_id: str, args, kwargs) -> None:
        """Run by the worker if the task executes successfully"""

        # we want to complete our own UserImportBook job, not ChildJob
        model = apps.get_model(f"bookwyrm.{kwargs['job_type']}", require_ready=True)
        subtask = model.objects.get(id=kwargs["child_id"])
        subtask.complete_job()


def book_item_key(book_data: dict[str, Any], index: int) -> str:
    """stable identity for a book inside the archive"""
    edition = book_data.get("edition") or {}
    return f"book:{edition.get('id') or index}"


def post_item_key(book_key: str, status_type: str, status: dict, index: int) -> str:
    """stable identity for a status inside the archive"""
    return f"post:{status.get('id') or f'{book_key}-{status_type}-{index}'}"


def relationship_item_key(relationship: str, remote_id: str) -> str:
    """stable identity for a follow/block inside the archive"""
    return f"rel:{relationship}:{remote_id}"


# (model field on book_data, UserImportPost.StatusType value, required setting)
POST_CATEGORIES = [
    ("comments", UserImportPost.StatusType.COMMENT, "include_comments"),
    ("quotations", UserImportPost.StatusType.QUOTE, "include_quotations"),
    ("reviews", UserImportPost.StatusType.REVIEW, "include_reviews"),
]


def prior_item_is_complete(
    job: BookwyrmImportJob, model, **filters
) -> bool:
    """has this archive item already been confirmed by an earlier
    generation of this import?"""
    return job.lineage_child(model, status=Job.Status.COMPLETE, **filters).exists()


def create_post_children(
    job: BookwyrmImportJob,
    book: models.Edition,
    book_key: str,
    book_data: dict[str, Any],
) -> None:
    """create the stable set of status child tasks for one book.
    Items already completed by an earlier generation are skipped, and
    re-delivered or replayed runs reuse the same rows."""

    for category, status_type, required_setting in POST_CATEGORIES:
        if required_setting not in job.required:
            continue
        for index, status in enumerate(book_data.get(category) or []):
            key = post_item_key(book_key, status_type, status, index)
            if prior_item_is_complete(job, UserImportPost, item_key=key):
                continue
            UserImportPost.objects.get_or_create(
                parent_job=job,
                item_key=key,
                defaults={
                    "json": status,
                    "book": book,
                    "status_type": status_type,
                },
            )


def create_book_children(job: BookwyrmImportJob, import_data: dict[str, Any]) -> None:
    """create the stable set of book and status child tasks.

    Books resolved by an earlier generation only re-claim statuses that
    never completed; failed/unconfirmed books get a fresh task row."""

    for index, book_data in enumerate(import_data.get("books") or []):
        key = book_item_key(book_data, index)
        prior = (
            job.lineage_child(UserImportBook, item_key=key)
            .filter(status=Job.Status.COMPLETE, book__isnull=False)
            .order_by("-updated_date")
            .first()
        )
        if prior is not None:
            # the book and its shelves/readthroughs/lists are done,
            # only orphaned statuses (if any) need to be claimed
            create_post_children(job, prior.book, key, book_data)
            continue

        UserImportBook.objects.get_or_create(
            parent_job=job,
            item_key=key,
            defaults={"book_data": book_data},
        )


def create_relationship_children(
    job: BookwyrmImportJob, import_data: dict[str, Any]
) -> None:
    """create the stable set of follow/block child tasks"""

    categories = [
        ("follow", "include_follows", "follows"),
        ("block", "include_blocks", "blocks"),
    ]
    for relationship, required_setting, data_key in categories:
        if required_setting not in job.required:
            continue
        for remote_id in import_data.get(data_key) or []:
            item_key = relationship_item_key(relationship, remote_id)
            if prior_item_is_complete(
                job, UserImportRelationship, item_key=item_key
            ):
                continue
            UserImportRelationship.objects.get_or_create(
                parent_job=job,
                item_key=item_key,
                defaults={
                    "relationship": relationship,
                    "remote_id": remote_id,
                },
            )


def origin_is_reachable(import_data: dict[str, Any]) -> bool:
    """check whether the source instance is still online.
    If not, we don't bother trying to pull book data from it."""

    try:
        url_parts = urlparse(import_data.get("id"))
        url = f"{url_parts.scheme}://{url_parts.netloc}"
        resp = requests.head(
            url,
            headers={
                "User-Agent": settings.USER_AGENT,
            },
            timeout=settings.QUERY_TIMEOUT,
        )
        return resp.ok
    except (
        EndpointConnectionError,
        requests.exceptions.ConnectionError,
        ConnectionRefusedError,
    ):
        return False


@app.task(queue=IMPORTS, base=ImportUserTask)
def start_import_task(**kwargs) -> None:
    """Build the stable manifest of child tasks and dispatch it.

    Safe to run more than once (queue redelivery, worker restart): it
    reuses persisted import data and stable child identities, only
    dispatches tasks that have not been confirmed, and reconciles the
    parent job from persisted results at the end.
    We always import the books even if not assigning
    them to shelves, lists etc"""

    try:
        with transaction.atomic():
            # serialize concurrent (re)deliveries of the start task
            job = BookwyrmImportJob.objects.select_for_update().get(
                id=kwargs["job_id"]
            )

            if job.complete or job.status == Job.Status.STOPPED:
                return

            if not job.user.is_active or job.is_superseded:
                # user deactivated or a newer upload took over:
                # stop this generation in its tracks
                job.stop_job()
                return

            job.set_status(Job.Status.ACTIVE)

            if job.import_data is None:
                archive_file = job.archive_file
                archive_file.open("rb")
                with BookwyrmTarFile.open(
                    mode="r:gz", fileobj=archive_file
                ) as tar:
                    json_filename = next(
                        filter(lambda n: n.startswith("archive"), tar.getnames())
                    )
                    job.import_data = json.loads(
                        tar.read(json_filename).decode("utf-8")
                    )
                    job.save(update_fields=["import_data"])

                    # profile/settings/goals are applied a single time by
                    # the generation that first opens the archive, so a
                    # retry can't overwrite later changes
                    applies_user_data = not job.lineage.exclude(id=job.id).filter(
                        generation__lt=job.generation,
                        import_data__isnull=False,
                    ).exists()
                    if applies_user_data and job.is_live:
                        if "include_user_profile" in job.required:
                            update_user_profile(job.user, tar, job.import_data)
                        if "include_user_settings" in job.required:
                            update_user_settings(job.user, job.import_data)
                        if "include_goals" in job.required:
                            update_goals(job.user, job.import_data.get("goals", []))
                        if "include_saved_lists" in job.required:
                            upsert_saved_lists(
                                job.user, job.import_data.get("saved_lists", [])
                            )
                archive_file.close()

            # one stable manifest per category; each category is
            # independent so one failing class cannot block the others
            create_relationship_children(job, job.import_data)
            create_book_children(job, job.import_data)

            # empty manifest (nothing selected / empty categories):
            # converge the parent immediately instead of waiting for a
            # child callback
            job.evaluate_completion()

        # network and queue dispatch happen outside the row lock, once
        # the manifest has been committed
        if not job.complete and job.is_live:
            origin_ok = origin_is_reachable(job.import_data)

            # dispatch only unclaimed tasks; completed or in-flight
            # children are left alone
            for item in job.relationship_tasks.filter(status=Job.Status.PENDING):
                item.start_job()
            for item in job.book_tasks.filter(status=Job.Status.PENDING):
                item.start_job(origin_is_ok=origin_ok)
            for item in job.status_tasks.filter(status=Job.Status.PENDING):
                item.start_job()

    except Exception as err:
        logger.error(
            "User Import Job %s Failed with error: %s",
            kwargs["job_id"],
            err,
            exc_info=True,
        )
        job = BookwyrmImportJob.objects.filter(id=kwargs["job_id"]).first()
        if job is not None and not job.complete:
            job.set_status(Job.Status.FAILED)


def create_book_from_json(book_data: dict[str, Any]) -> models.Edition:
    """create a book from the JSON in the import file
    as a last resort if we can't find the book
    in this instance or in the source instance"""

    edition = book_data.get("edition")
    work = book_data.get("work")
    # make sure we have the authors in the local DB
    # replace the old author ids in the edition JSON
    edition["authors"] = []
    work["authors"] = []
    for author in book_data.get("authors"):
        instance = activitypub.parse(author).to_model(
            model=models.Author, save=True, overwrite=False
        )

        edition["authors"].append(instance.remote_id)
        work["authors"].append(instance.remote_id)

    # first we need the parent work to exist
    work["editions"] = []
    work_instance = activitypub.parse(work).to_model(
        model=models.Work, save=True, overwrite=False
    )

    # now we have a work we can add it to the edition
    # and create the edition model instance
    edition["work"] = work_instance.remote_id
    book = activitypub.parse(edition).to_model(
        model=models.Edition, save=True, overwrite=False
    )

    return book


@app.task(queue=IMPORTS, base=UserImportSubTask)
def import_book_task(**kwargs) -> None:
    """Take work and edition data,
    find or create the edition and work in the database"""

    task = UserImportBook.objects.get(id=kwargs["child_id"])
    job = BookwyrmImportJob.objects.get(id=task.parent_job_id)
    book_data = task.book_data

    if task.complete or not job.is_live:
        return

    book = None
    try:
        edition = book_data.get("edition")
        book = models.Edition.find_existing(edition)
        if not book:
            if kwargs.get("origin_is_ok"):
                # try importing from the instance the user is coming from
                remote_id = edition.get("id")
                connector = connector_manager.get_or_create_connector(remote_id)
                book = connector.get_or_create_book(remote_id)

            if not book:
                # import directly from the import JSON
                # this will not create a cover image
                book = create_book_from_json(book_data)

        task.book = book
        task.save(update_fields=["book"])

        if "include_shelves" in job.required:
            upsert_shelves(job.user, book, book_data.get("shelves"))

        if "include_readthroughs" in job.required:
            upsert_readthroughs(
                job.user, book.id, book_data.get("readthroughs")
            )

        if "include_lists" in job.required:
            upsert_lists(job.user, book.id, book_data.get("lists"))

    except Exception as err:
        # a single bad remote book fails just this child; other books,
        # statuses and relationships keep going. Do not create statuses
        # pointing at a missing book.
        logger.error(
            "Book Import Task %s for Job %s Failed with error: %s",
            task.id,
            job.id,
            err,
            exc_info=True,
        )
        task.fail_reason = _("Unknown error importing book")
        task.save(update_fields=["fail_reason"])
        task.set_status(Job.Status.FAILED)
        return

    # Now import statuses
    # These are also subtasks so that we can isolate anything that fails.
    # Stable item keys make replay and redelivery idempotent.
    book_key = task.item_key or book_item_key(book_data, 0)
    create_post_children(job, book, book_key, book_data)

    # only dispatch this book's unclaimed posts, not every post of
    # the parent job (which other book tasks may still be creating)
    for item in job.status_tasks.filter(book=book, status=Job.Status.PENDING):
        item.start_job()

    task.complete_job()


@app.task(queue=IMPORTS, base=UserImportSubTask)
def upsert_status_task(**kwargs) -> None:
    """Find or create book statuses"""

    task = UserImportPost.objects.get(id=kwargs["child_id"])
    job = BookwyrmImportJob.objects.get(id=task.parent_job_id)
    user: models.User = job.user
    status: dict[str, Any] = task.json
    status_class = (
        models.Review
        if task.status_type == "review"
        else models.Quotation
        if task.status_type == "quote"
        else models.Comment
    )

    if task.complete or not job.is_live:
        return

    try:
        # only add statuses if this is the same user
        if is_alias(user, status.get("attributedTo")):
            status["attributedTo"] = user.remote_id
            status["to"] = update_followers_address(user, status["to"])
            status["cc"] = update_followers_address(user, status["cc"])
            status[
                "replies"
            ] = {}  # this parses incorrectly but we can't set it without knowing the new id
            status["inReplyToBook"] = task.book.remote_id
            parsed: activitypub.Comment = activitypub.parse(status)
            if not status_already_exists(
                user, parsed
            ):  # don't duplicate posts on multiple import
                instance = parsed.to_model(
                    model=status_class, save=True, overwrite=True
                )

                for val in [
                    "progress",
                    "progress_mode",
                    "position",
                    "endposition",
                    "position_mode",
                ]:
                    if status.get(val):
                        instance.val = status[val]

                instance.remote_id = instance.get_remote_id()  # update the remote_id
                instance.save()  # save and broadcast

            task.complete_job()

        else:
            logger.warning(
                "User not authorized to import statuses, or status is tombstone"
            )
            task.fail_reason = _("unauthorized")
            task.save(update_fields=["fail_reason"])
            task.set_status("failed")

    except Exception as err:
        logger.error("User Status Import Task %s Failed with error: %s", task.id, err)
        task.fail_reason = _("Unknown error importing book status")
        task.save(update_fields=["fail_reason"])
        task.set_status("failed")


def upsert_readthroughs(
    user: models.User, book_id: int, data: list[dict[str, Any]]
) -> None:
    """Take a JSON string of readthroughs and
    find or create the instances in the database"""

    for read_through in data:
        obj = {}
        keys = [
            "progress_mode",
            "start_date",
            "finish_date",
            "stopped_date",
            "is_active",
        ]
        for key in keys:
            obj[key] = read_through[key]
        obj["user_id"] = user.id
        obj["book_id"] = book_id

        existing = models.ReadThrough.objects.filter(**obj).first()
        if not existing:
            models.ReadThrough.objects.create(**obj)


def upsert_lists(
    user: models.User,
    book_id: int,
    lists: list[dict[str, Any]],
) -> None:
    """Take a list of objects each containing
    a list and list item as AP objects

    Because we are creating new IDs we can't assume the id
    will exist or be accurate, so we only use to_model for
    adding new items after checking whether they exist  .

    """

    book = models.Edition.objects.get(id=book_id)

    for blist in lists:
        booklist = models.List.objects.filter(name=blist["name"], user=user).first()
        if not booklist:
            blist["owner"] = user.remote_id
            parsed = activitypub.parse(blist)
            booklist = parsed.to_model(model=models.List, save=True, overwrite=True)

            booklist.privacy = blist["privacy"]
            booklist.save()

        item = models.ListItem.objects.filter(edition=book, book_list=booklist).exists()
        if not item:
            count = booklist.editions.count()
            notes = blist["list_item"].get("notes", "")
            approved = blist["list_item"].get("approved", False)
            models.ListItem.objects.create(
                edition=book,
                book_list=booklist,
                user=user,
                notes=notes,
                approved=approved,
                order=count + 1,
            )


def upsert_shelves(
    user: models.User, book: models.Book, shelves: list[dict[str, Any]]
) -> None:
    """Take shelf JSON objects and create
    DB entries if they don't already exist"""

    for shelf in shelves:
        book_shelf = models.Shelf.objects.filter(name=shelf["name"], user=user).first()

        if not book_shelf:
            book_shelf = models.Shelf.objects.create(name=shelf["name"], user=user)

        # add the book as a ShelfBook if needed
        if not models.ShelfBook.objects.filter(
            book=book, shelf=book_shelf, user=user
        ).exists():
            models.ShelfBook.objects.create(
                book=book, shelf=book_shelf, user=user, shelved_date=timezone.now()
            )


# user updates
##############


def update_user_profile(
    user: models.User, tar: BookwyrmTarFile, data: dict[str, Any]
) -> None:
    """update the user's profile from import data"""
    name = data.get("name", None)
    username = data.get("preferredUsername")
    user.name = name if name else username
    user.summary = strip_tags(data.get("summary", None))
    user.save(update_fields=["name", "summary"])
    if data["icon"].get("url"):
        avatar_filename = next(filter(lambda n: n.startswith("avatar"), tar.getnames()))
        tar.write_image_to_file(avatar_filename, user.avatar)


def update_user_settings(user: models.User, data: dict[str, Any]) -> None:
    """update the user's settings from import data"""

    update_fields = ["manually_approves_followers", "hide_follows", "discoverable"]

    activitypub_fields = [
        ("manuallyApprovesFollowers", "manually_approves_followers"),
        ("hideFollows", "hide_follows"),
        ("discoverable", "discoverable"),
    ]

    for activitypub_field, bookwyrm_field in activitypub_fields:
        setattr(user, bookwyrm_field, data[activitypub_field])

    bookwyrm_fields = [
        "show_goal",
        "show_suggested_users",
        "default_post_privacy",
        "preferred_timezone",
    ]

    for field in bookwyrm_fields:
        update_fields.append(field)
        setattr(user, field, data["settings"][field])

    user.save(update_fields=update_fields)


def update_goals(user: models.User, data: list[dict[str, Any]]) -> None:
    """update the user's goals from import data"""

    for goal in data:
        # edit the existing goal if there is one
        existing = models.AnnualGoal.objects.filter(
            year=goal["year"], user=user
        ).first()
        if existing:
            for k in goal.keys():
                setattr(existing, k, goal[k])
            existing.save()
        else:
            goal["user"] = user
            models.AnnualGoal.objects.create(**goal)


def upsert_saved_lists(user: models.User, remote_ids: list[str]) -> None:
    """Take a list of remote ids and add as saved lists"""

    for remote_id in remote_ids:
        book_list = activitypub.resolve_remote_id(remote_id, models.List)
        if book_list:
            user.saved_lists.add(book_list)


@app.task(queue=IMPORTS, base=UserImportSubTask)
def import_user_relationship_task(**kwargs) -> None:
    """import a user follow or block from an import file"""

    task = UserImportRelationship.objects.get(id=kwargs["child_id"])
    job = BookwyrmImportJob.objects.get(id=task.parent_job_id)

    # a re-delivered, completed task must never send follow/block
    # side effects a second time, and stale generations must not run
    if task.complete or not job.is_live:
        return

    try:
        if task.relationship == "follow":
            followee = activitypub.resolve_remote_id(task.remote_id, models.User)
            if followee:
                (
                    follow_request,
                    created,
                ) = models.UserFollowRequest.objects.get_or_create(
                    user_subject=job.user,
                    user_object=followee,
                )

                if not created:
                    # this request probably failed to connect with the remote
                    # and should save to trigger a re-broadcast
                    follow_request.save()

                task.complete_job()

            else:
                logger.error(
                    "Could not resolve import user %s follow task %s",
                    task.remote_id,
                    task.id,
                )
                task.fail_reason = _("connection_error")
                task.save(update_fields=["fail_reason"])
                task.set_status("failed")

        elif task.relationship == "block":
            user_object = activitypub.resolve_remote_id(task.remote_id, models.User)
            if user_object:
                exists = models.UserBlocks.objects.filter(
                    user_subject=job.user, user_object=user_object
                ).exists()
                if not exists:
                    models.UserBlocks.objects.create(
                        user_subject=job.user, user_object=user_object
                    )
                    # remove the blocked users's lists from the groups
                    models.List.remove_from_group(job.user, user_object)
                    # remove the blocked user from all blocker's owned groups
                    models.GroupMember.remove(job.user, user_object)

                task.complete_job()

            else:
                logger.error(
                    "Could not resolve user %s block task %s", task.remote_id, task.id
                )
                task.fail_reason = _("connection_error")
                task.save(update_fields=["fail_reason"])
                task.set_status("failed")

        else:
            logger.error(
                "Invalid relationship type %s specified in user import task %s",
                task.relationship,
                task.id,
            )
            task.fail_reason = _("invalid_relationship")
            task.save(update_fields=["fail_reason"])
            task.set_status("failed")

    except Exception as err:
        logger.error(
            "User Import Relationship Task %s Failed with error: %s", task.id, err
        )
        task.fail_reason = _("Unkown error importing relationship")
        task.save(update_fields=["fail_reason"])
        task.set_status("failed")


# recovery
##########


def _unclaimed_children(queryset, stale_before):
    """pending children that either never got a queued message or whose
    queued message is older than the stale-task threshold"""
    return queryset.filter(status=Job.Status.PENDING).filter(
        Q(task_id__isnull=True) | Q(updated_date__lt=stale_before)
    )


@app.task(queue=MISC)
def recover_import_jobs_task(**kwargs) -> None:
    """Reconcile unfinished user imports after queue redelivery, worker
    crashes or a service restart.

    Only unconfirmed children are re-claimed: completed children (in any
    generation) are left untouched, failed children wait for an explicit
    retry, and the parent job converges from its persisted results.
    Intended to be run on an interval (like other maintenance tasks)."""

    stale_before = timezone.now() - STALE_TASK_LIMIT
    job_ids = list(
        BookwyrmImportJob.objects.filter(complete=False).values_list(
            "id", flat=True
        )
    )
    for job_id in job_ids:
        try:
            recover_import_job(job_id, stale_before)
        except Exception:
            logger.exception(
                "Failed to recover user import job %s", job_id
            )


def recover_import_job(job_id: int, stale_before=None) -> None:
    """reconcile a single import job"""
    job = BookwyrmImportJob.objects.get(id=job_id)
    if job.complete:
        return

    if stale_before is None:
        stale_before = timezone.now() - STALE_TASK_LIMIT

    if not job.user.is_active or job.is_superseded:
        # deactivated user or a newer upload: this generation is over
        job.stop_job()
        job.delete_archive_if_unreferenced()
        return

    # the start task itself never got through (no manifest persisted)
    if job.import_data is None:
        if job.task_id is None or job.updated_date < stale_before:
            start_import_task.delay(job_id=job.id)
        return

    # children a worker claimed but never confirmed: reclaim them
    for child in job.child_jobs.filter(status=Job.Status.ACTIVE).filter(
        updated_date__lt=stale_before
    ):
        child.reopen()

    # re-queue pending children whose message is missing or stale.
    # failed children are NOT auto-retried here.
    for item in _unclaimed_children(job.relationship_tasks, stale_before):
        item.start_job()
    for item in _unclaimed_children(job.book_tasks, stale_before):
        item.start_job(origin_is_ok=False)
    for item in _unclaimed_children(job.status_tasks, stale_before):
        item.start_job()

    # converge the parent from persisted child state even if the last
    # completion callback was lost
    job.evaluate_completion()


# utilities
###########


def update_followers_address(user: models.User, audiences: list[str]) -> list[str]:
    """statuses to or cc followers need to have the followers
    address updated to the new local user"""

    for i, audience in enumerate(audiences):
        if audience.rsplit("/")[-1] == "followers":
            audiences[i] = user.followers_url

    return audiences


def is_alias(user: models.User, remote_id: str) -> bool:
    """check that the user is listed as moved_to
    or also_known_as in the remote user's profile"""

    if not remote_id:
        return False

    remote_user = activitypub.resolve_remote_id(
        remote_id=remote_id, model=models.User, save=False
    )

    if remote_user:
        if getattr(remote_user, "moved_to", None) is not None:
            return user.remote_id == remote_user.moved_to

        if hasattr(remote_user, "also_known_as"):
            return user in remote_user.also_known_as.all()

    return False


def status_already_exists(user: models.User, status: activitypub.Comment) -> bool:
    """check whether this status has already been published
    by this user. We can't rely on to_model() because it
    only matches on remote_id, which we have to change
    *after* saving because it needs the primary key (id)"""

    return models.Status.objects.filter(
        user=user, content=status.content, published_date=status.published
    ).exists()
