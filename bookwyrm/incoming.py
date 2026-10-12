"""idempotent processing of incoming ActivityPub activities

A remote instance may redeliver an activity after a timeout, and a Celery
worker may be killed after the domain objects were written but before the
task was acknowledged. Both situations used to run the whole activity a
second time. This module makes the inbox processing resumable:

* the delivery is claimed exactly once (``InboxActivity``), after the
  signature and block checks have passed;
* the domain change for Create/Update/Delete/Follow/Add/Remove is applied
  once from that record, in replayable stages;
* a failure while fetching remote data leaves the delivery *not applied*
  and rolls partial writes back, so a retry starts clean;
* if the process dies after the domain commit, the retry only replays the
  missing notifications/broadcasts;
* activities that arrive out of order are refused according to the
  generation asserted by the remote object, so a late Update/Undo/Follow
  can never resurrect a newer Delete/Unfollow/ban state.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import timedelta, timezone as dt_timezone

import dateutil.parser
import requests
from django.core.exceptions import PermissionDenied
from django.db import IntegrityError, transaction
from django.utils import timezone

from bookwyrm import activitypub, models
from bookwyrm.activitypub.base_activity import ActivitySerializerError, item_lock
from bookwyrm.connectors import ConnectorException
from bookwyrm.tasks import app, INBOX

logger = logging.getLogger(__name__)

# connectivity problems are transient: nothing is applied and the Celery
# task retries, continuing from the missing stage
RECOVERABLE_ERRORS = (ConnectorException, requests.RequestException)

# how long (seconds) the nth attempt waits before retrying, capped at an hour
RETRY_BACKOFF = [30, 60, 120, 300, 600, 1800, 3600]

# verbs that create/update state vs verbs that remove it, per ordering group
_RIVAL_RULES = {
    "object": ({"Delete"}, {"Create", "Update"}),
    "follow": ({"Undo"}, {"Follow"}),
    "block": ({"Undo"}, {"Block"}),
    "like": ({"Undo"}, {"Like"}),
    "announce": ({"Undo"}, {"Announce"}),
    "collection-item": ({"Remove"}, {"Add"}),
}


class RecoverableDelivery(Exception):
    """the delivery could not be finished but a retry may succeed"""

    def __init__(self, message, *, countdown=60):
        super().__init__(message)
        self.countdown = countdown


class DeliveryConflict(Exception):
    """the same activity id was redelivered with another sender/body"""


def canonical_digest(activity_json: dict) -> str:
    """stable digest of an activity payload, used for task-side claims"""
    return hashlib.sha256(
        json.dumps(activity_json, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


# ---------------------------------------------------------------------------
# claim
# ---------------------------------------------------------------------------


def _target_id(activity_json: dict) -> str:
    obj = activity_json.get("object")
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        return obj.get("id") or ""
    return ""


def _parse_generation(value):
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = dateutil.parser.parse(value)
    except (ValueError, TypeError, OverflowError):
        return None
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, dt_timezone.utc)
    return parsed.astimezone(dt_timezone.utc)


def claimed_generation(activity_json: dict):
    """the time the remote object asserts for this change, if any"""
    obj = activity_json.get("object")
    candidates = [
        activity_json.get("published"),
        activity_json.get("updated"),
    ]
    if isinstance(obj, dict):
        candidates += [obj.get("updated"), obj.get("published")]
    for candidate in candidates:
        if parsed := _parse_generation(candidate):
            return parsed
    return None


def ordering_group(activity_json: dict) -> str:
    """activities that mutate the same remote object share a group"""
    verb = activity_json.get("type")
    actor = activity_json.get("actor") or ""
    obj = activity_json.get("object")

    if verb in ("Create", "Update", "Delete"):
        target = _target_id(activity_json)
        return f"object:{target}" if target else ""

    if verb in ("Follow", "Block", "Like", "Announce"):
        if isinstance(obj, str):
            return f"{verb.lower()}:{actor}|{obj}"

    if verb in ("Add", "Remove"):
        if isinstance(obj, dict) and obj.get("id"):
            return f"collection-item:{obj['id']}"

    if verb == "Move":
        return f"user:{actor}" if actor else ""

    if verb == "Undo" and isinstance(obj, dict):
        # order the Undo against the original activity in the same group
        return ordering_group(obj)

    return ""


def claim(
    activity_json: dict,
    *,
    signature_key_id: str = "",
    body_digest: str = "",
):
    """persistently claim a signed, un-blocked delivery.

    Returns the ``InboxActivity`` row (new or existing) or ``None`` when the
    activity carries no id and therefore cannot be de-duplicated. A
    redelivery with the same id but a different sender/body raises
    ``DeliveryConflict``.
    """
    activity_id = activity_json.get("id")
    if not activity_id:
        return None

    sender = activity_json.get("actor") or ""
    generation = claimed_generation(activity_json)
    defaults = {
        "sender": sender,
        "activity_type": activity_json.get("type") or "",
        "signature_key_id": signature_key_id or "",
        "ordering_group": ordering_group(activity_json),
        "target_remote_id": _target_id(activity_json),
        "generation": generation.isoformat() if generation else "",
        "payload": activity_json,
    }
    if body_digest:
        defaults["body_digest"] = body_digest

    try:
        delivery, created = models.InboxActivity.objects.get_or_create(
            activity_id=activity_id, defaults=defaults
        )
    except IntegrityError:
        delivery = models.InboxActivity.objects.get(activity_id=activity_id)
        created = False

    if not created:
        if sender and delivery.sender and sender != delivery.sender:
            logger.warning(
                "Rejecting redelivery of %s with mismatched sender %s != %s",
                activity_id,
                sender,
                delivery.sender,
            )
            raise DeliveryConflict(activity_id)
        if body_digest and delivery.body_digest and body_digest != delivery.body_digest:
            logger.warning(
                "Rejecting redelivery of %s with mismatched body", activity_id
            )
            raise DeliveryConflict(activity_id)
    return delivery


# ---------------------------------------------------------------------------
# generation guard
# ---------------------------------------------------------------------------


def _structural_stale(activity_json, generation_dt):
    """refuse activities that would resurrect a newer local state"""
    verb = activity_json.get("type")
    actor = activity_json.get("actor")
    target = _target_id(activity_json)
    actor_user = models.User.find_existing_by_remote_id(actor) if actor else None

    # a user that was banned/deleted after the delivery was accepted must
    # not be able to write anything; self-Delete is what performs the ban
    if actor_user:
        if actor_user.deleted:
            if not (verb == "Delete" and target == actor):
                return models.InboxActivity.RejectionReason.GONE_ACTOR

        # activities that predate the account migration are too late
        move = (
            models.MoveUser.objects.filter(user=actor_user)
            .order_by("-created_date")
            .first()
        )
        if (
            move
            and generation_dt
            and generation_dt < move.created_date
            and not (verb == "Delete" and target == actor)
        ):
            return models.InboxActivity.RejectionReason.STALE_GENERATION

    # status tombstones: a late Create/Update must not un-delete an object,
    # and a second Delete for an already-deleted object needs no work
    if verb in ("Create", "Update", "Delete") and target:
        status = models.Status.objects.filter(remote_id=target).first()
        if status is not None and status.deleted:
            return models.InboxActivity.RejectionReason.STALE_GENERATION

    # a Move for an account that migrated elsewhere is out of date
    if verb == "Move" and actor_user and actor_user.moved_to:
        new_target = activity_json.get("target")
        if new_target != actor_user.moved_to:
            return models.InboxActivity.RejectionReason.STALE_GENERATION

    return None


def _rival_stale(activity_json, delivery, generation_dt):
    """refuse activities older than a completed rival in the same group"""
    if not generation_dt or not delivery.ordering_group:
        return None

    group = delivery.ordering_group
    kind = group.split(":", 1)[0]
    rule = _RIVAL_RULES.get(kind)
    if not rule:
        return None

    destructive, constructive = rule
    verb = activity_json.get("type")
    if verb not in destructive and verb not in constructive:
        return None

    incoming_iso = generation_dt.isoformat()
    newer = models.InboxActivity.objects.filter(
        ordering_group=group,
        completed_at__isnull=False,
        generation__gt=incoming_iso,
    ).exclude(pk=delivery.pk)
    for prior in newer:
        if (verb in constructive and prior.activity_type in destructive) or (
            verb in destructive and prior.activity_type in constructive
        ):
            return models.InboxActivity.RejectionReason.STALE_GENERATION
    return None


# ---------------------------------------------------------------------------
# stage replay
# ---------------------------------------------------------------------------


def _reconcile(activity_json):
    """idempotently replay only the notifications/broadcasts of an activity.

    This runs when a retry found the domain change already committed, so it
    must never create domain objects.
    """
    verb = activity_json.get("type")
    target = _target_id(activity_json)

    if verb in ("Create", "Update") and target:
        status = (
            models.Status.objects.filter(remote_id=target).select_subclasses().first()
        )
        if status is not None and not status.deleted:
            from bookwyrm.models.notification import notify_user_on_mention

            notify_user_on_mention(sender=models.Status, instance=status)

    elif verb == "Delete" and target:
        models.Notification.objects.filter(related_status__remote_id=target).delete()

    elif verb == "Follow":
        subject = models.User.find_existing_by_remote_id(activity_json.get("actor"))
        object_user = models.User.find_existing_by_remote_id(target)
        if not subject or not object_user:
            return
        follow = models.UserFollows.objects.filter(
            user_subject=subject, user_object=object_user
        ).first()
        if follow:
            models.Notification.notify(
                object_user,
                subject,
                notification_type=models.NotificationType.FOLLOW,
                read=False,
            )
            # a remote follow of a local user is answered with an Accept,
            # which may have been lost to the crash
            if object_user.local:
                accept = activitypub.Accept(
                    id=follow.get_accept_reject_id("accepts"),
                    actor=object_user.remote_id,
                    object=follow.to_activity(),
                ).serialize()
                follow.broadcast(accept, object_user)
            return
        request = models.UserFollowRequest.objects.filter(
            user_subject=subject, user_object=object_user
        ).first()
        if request and object_user.local:
            models.Notification.notify(
                object_user,
                subject,
                notification_type=models.NotificationType.FOLLOW_REQUEST,
                read=False,
            )

    elif verb == "Undo" and isinstance(activity_json.get("object"), dict):
        inner = activity_json["object"]
        if inner.get("type") == "Follow":
            subject = models.User.find_existing_by_remote_id(inner.get("actor"))
            object_user = models.User.find_existing_by_remote_id(inner.get("object"))
            if subject and object_user:
                models.Notification.unnotify(
                    object_user,
                    subject,
                    notification_type=models.NotificationType.FOLLOW,
                )

    elif verb == "Add" and isinstance(activity_json.get("object"), dict):
        item_id = activity_json["object"].get("id")
        if item_id:
            item = models.ListItem.objects.filter(remote_id=item_id).first()
            if item is not None:
                from bookwyrm.models.notification import notify_user_on_list_item_add

                notify_user_on_list_item_add(
                    sender=models.ListItem, instance=item, created=True
                )


# ---------------------------------------------------------------------------
# pipeline
# ---------------------------------------------------------------------------


def process(
    activity_json: dict,
    allow_external_connections: bool = True,
    *,
    delivery=None,
    signature_key_id: str = "",
    body_digest: str = "",
):
    """apply a signed inbox activity exactly once, resuming missing stages.

    Both the synchronous small-activity path and the asynchronous Celery
    task call this, so they share the same delivery record.
    """
    activity_id = activity_json.get("id")
    if delivery is None and activity_id:
        delivery = claim(
            activity_json,
            signature_key_id=signature_key_id,
            body_digest=body_digest,
        )

    # activities without ids keep the legacy, un-deduplicated behavior
    if delivery is None:
        activitypub.parse(activity_json).action(
            allow_external_connections=allow_external_connections
        )
        return None

    # always work off the freshest persisted state
    delivery.refresh_from_db()
    if delivery.is_terminal:
        return delivery

    try:
        activity = activitypub.parse(activity_json)
    except ActivitySerializerError:
        delivery.reject(
            models.InboxActivity.RejectionReason.INVALID_ACTIVITY,
            f"unparseable activity: {activity_json.get('type')}",
        )
        raise

    try:
        # separate namespace from per-object locks, which use the same id for
        # some activities (e.g. an Announce id equals the boost object id)
        with item_lock(activity_id, prefix="inbox-delivery"):
            delivery.refresh_from_db()
            if delivery.is_terminal:
                return delivery

            was_applied = delivery.applied_at is not None
            delivery.status = models.InboxActivity.Status.PROCESSING
            delivery.attempts += 1
            delivery.save(update_fields=["status", "attempts", "updated_date"])

            generation_dt = claimed_generation(activity_json)

            # stage: resolve / validate generations, before any domain write
            if not delivery.resolved_at:
                reason = _structural_stale(
                    activity_json, generation_dt
                ) or _rival_stale(activity_json, delivery, generation_dt)
                if reason:
                    delivery.reject(reason)
                    logger.info(
                        "Refusing %s for %s: %s",
                        activity_json.get("type"),
                        delivery.target_remote_id,
                        reason,
                    )
                    return delivery
                delivery.resolved_at = timezone.now()
                delivery.save(update_fields=["resolved_at", "updated_date"])

            try:
                # stage: apply the domain change exactly once. One
                # transaction means a failed remote fetch rolls all partial
                # writes back, leaving the delivery not-applied
                if not was_applied:
                    try:
                        with transaction.atomic():
                            activity.action(
                                allow_external_connections=allow_external_connections
                            )
                    except ActivitySerializerError as err:
                        if not allow_external_connections:
                            # sync path control flow: hand off to the async
                            # task and leave the delivery not-applied
                            _mark_pending(delivery, str(err))
                            raise
                        delivery.reject(
                            models.InboxActivity.RejectionReason.INVALID_ACTIVITY,
                            str(err),
                        )
                        raise
                    except IntegrityError as err:
                        # e.g. following a user that blocks the sender
                        delivery.reject(
                            models.InboxActivity.RejectionReason.INVALID_ACTIVITY,
                            str(err),
                        )
                        raise
                    delivery.applied_at = timezone.now()
                    delivery.status = models.InboxActivity.Status.APPLIED
                    delivery.save(
                        update_fields=["applied_at", "status", "updated_date"]
                    )
                else:
                    # stage: object already committed by a crashed attempt;
                    # only replay the missing after-effects, never the change
                    _reconcile(activity_json)

                # stage: supplementation complete
                delivery.completed_at = timezone.now()
                delivery.status = models.InboxActivity.Status.COMPLETED
                delivery.error = ""
                delivery.save(
                    update_fields=["completed_at", "status", "error", "updated_date"]
                )

            except RecoverableDelivery as err:
                _mark_pending(delivery, str(err))
                raise RecoverableDelivery(
                    str(err),
                    countdown=RETRY_BACKOFF[min(max(delivery.attempts - 1, 0), 6)],
                )
            except RECOVERABLE_ERRORS as err:
                # nothing was applied; retry without half-written rows
                _mark_pending(delivery, str(err))
                raise RecoverableDelivery(
                    str(err),
                    countdown=RETRY_BACKOFF[min(max(delivery.attempts - 1, 0), 6)],
                )
            except PermissionDenied as err:
                delivery.reject(
                    models.InboxActivity.RejectionReason.INVALID_ACTIVITY, str(err)
                )
                raise

    except ActivitySerializerError as err:
        if "Lock expired" in str(err):
            # another worker is still applying the delivery; retry later
            raise RecoverableDelivery(str(err), countdown=60)
        raise

    return delivery


def _mark_pending(delivery, error):
    """roll the delivery back to the not-applied, retryable result"""
    delivery.status = models.InboxActivity.Status.PENDING
    delivery.error = error[:10000]
    delivery.save(update_fields=["status", "error", "updated_date"])


# a delivery whose last update is older than this is assumed to have been
# abandoned by a killed worker (longer than the longest object lock)
RECOVERY_STALE_MINUTES = 15


@app.task(queue=INBOX)
def recover_stale_deliveries_task():
    """re-enqueue accepted deliveries abandoned by a crashed/restarted worker.

    Unlike a redelivery or a Celery retry, the process restart itself triggers
    this sweep: accepted but unfinished deliveries carry their payload in the
    ledger, so processing resumes from the missing stage without any help
    from the sending instance.
    """
    from bookwyrm.views.inbox import activity_task

    cutoff = timezone.now() - timedelta(minutes=RECOVERY_STALE_MINUTES)
    stale = (
        models.InboxActivity.objects.exclude(
            status__in=[
                models.InboxActivity.Status.COMPLETED,
                models.InboxActivity.Status.REJECTED,
            ]
        )
        .filter(updated_date__lt=cutoff)
        .order_by("created_date")
    )
    enqueued = 0
    for delivery in stale:
        if not delivery.payload:
            logger.warning(
                "Cannot resume payload-less delivery %s", delivery.activity_id
            )
            continue
        activity_task.apply_async(args=[delivery.payload])
        enqueued += 1
    return enqueued
