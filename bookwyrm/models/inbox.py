"""persistent ledger for incoming ActivityPub deliveries

Every signed, un-blocked inbox delivery is "claimed" once, keyed by the
activity id. The row tracks how far the delivery has been processed so that
redeliveries, Celery retries and process restarts resume only the missing
stage instead of re-applying the domain change.
"""

from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _


class InboxActivity(models.Model):
    """one accepted delivery of one activity, applied at most once"""

    class Status(models.TextChoices):
        """lifecycle of the delivery"""

        # accepted and validated, but no domain change has been committed
        PENDING = "PENDING", _("Pending")
        # a worker is currently resuming the delivery
        PROCESSING = "PROCESSING", _("Processing")
        # the domain change is committed; after-effects may still be pending
        APPLIED = "APPLIED", _("Applied")
        # domain change and all after-effects are done
        COMPLETED = "COMPLETED", _("Completed")
        # permanently refused; retries must not apply anything
        REJECTED = "REJECTED", _("Rejected")

    class Result(models.TextChoices):
        """queryable outcome of the delivery"""

        # nothing was applied; a later retry may still succeed
        NOT_APPLIED = "not_applied", _("Not applied")
        # domain objects were committed; only after-effects remain
        APPLIED_PENDING = "applied_pending", _("Applied, pending supplementation")
        # the delivery was permanently refused
        REJECTED = "rejected", _("Rejected")
        # applied and fully supplemented
        COMPLETED = "completed", _("Completed")

    class RejectionReason(models.TextChoices):
        """why a delivery was permanently refused"""

        # the activity is older than a already-applied newer generation
        STALE_GENERATION = "stale_generation", _("Stale generation")
        # the payload can't be serialized into a known activity/object
        INVALID_ACTIVITY = "invalid_activity", _("Invalid activity")
        # same activity id presented by another sender or with another body
        CONFLICT = "conflict", _("Conflicting redelivery")
        # the sender has been banned/deleted since the delivery was accepted
        GONE_ACTOR = "gone_actor", _("Sender is gone")

    # claim identity: activity id + sender + body digest + signature facts
    activity_id = models.CharField(max_length=255, unique=True)
    sender = models.CharField(max_length=255, db_index=True)
    activity_type = models.CharField(max_length=64, default="")
    body_digest = models.CharField(max_length=64, blank=True, default="")
    signature_key_id = models.CharField(max_length=255, blank=True, default="")
    # the accepted payload, so a restarted worker can resume the delivery
    # without waiting for the remote instance to redeliver it
    payload = models.JSONField(default=dict, blank=True)

    # ordering bookkeeping: activities acting on the same object share a
    # group, and the generation is the time asserted by the remote object
    ordering_group = models.CharField(max_length=700, blank=True, default="")
    target_remote_id = models.CharField(max_length=255, blank=True, default="")
    generation = models.CharField(max_length=40, blank=True, default="")

    status = models.CharField(
        max_length=32, choices=Status.choices, default=Status.PENDING, db_index=True
    )
    rejection_reason = models.CharField(
        max_length=32, choices=RejectionReason.choices, blank=True, default=""
    )
    error = models.TextField(blank=True, default="")
    attempts = models.PositiveIntegerField(default=0)

    # replayable stages; a timestamp means the stage has finished
    queued_at = models.DateTimeField(null=True, blank=True)
    resolved_at = models.DateTimeField(null=True, blank=True)
    applied_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    rejected_at = models.DateTimeField(null=True, blank=True)

    created_date = models.DateTimeField(auto_now_add=True)
    updated_date = models.DateTimeField(auto_now=True)

    class Meta:
        """ordering and indexes for the ledger"""

        indexes = [
            models.Index(fields=["ordering_group"]),
            models.Index(fields=["target_remote_id"]),
        ]

    @property
    def result(self) -> str:
        """coarse, queryable outcome of the delivery"""
        if self.status == self.Status.REJECTED:
            return self.Result.REJECTED
        if self.completed_at:
            return self.Result.COMPLETED
        if self.applied_at:
            return self.Result.APPLIED_PENDING
        return self.Result.NOT_APPLIED

    @property
    def is_terminal(self) -> bool:
        """nothing left to do, or permanently refused"""
        return self.status in (self.Status.COMPLETED, self.Status.REJECTED)

    def reject(self, reason: str, error: str = "") -> None:
        """permanently refuse the delivery"""
        self.status = self.Status.REJECTED
        self.rejection_reason = reason
        self.rejected_at = timezone.now()
        if error:
            self.error = error[:10000]
        self.save(
            update_fields=[
                "status",
                "rejection_reason",
                "rejected_at",
                "error",
                "updated_date",
            ]
        )

    def mark_queued(self) -> bool:
        """atomically record that an async task was queued for the delivery.

        Returns True for the caller that won the right to enqueue, so a
        redelivery never queues the same activity twice.
        """
        updated = (
            type(self)
            .objects.filter(pk=self.pk, queued_at__isnull=True)
            .update(queued_at=timezone.now())
        )
        if updated:
            self.queued_at = type(self).objects.get(pk=self.pk).queued_at
        return bool(updated)
