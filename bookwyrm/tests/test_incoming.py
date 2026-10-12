"""tests for the idempotent inbox delivery pipeline"""

import json
import pathlib
from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase, Client
from django.utils import timezone

from bookwyrm import incoming, models
from bookwyrm.activitypub import ActivitySerializerError


def _completed_delivery(
    activity_id,
    verb,
    group,
    generation,
    sender="https://example.com/users/rat",
    target="",
):
    now = timezone.now()
    return models.InboxActivity.objects.create(
        activity_id=activity_id,
        sender=sender,
        activity_type=verb,
        ordering_group=group,
        target_remote_id=target,
        generation=generation,
        status=models.InboxActivity.Status.COMPLETED,
        resolved_at=now - timedelta(minutes=2),
        applied_at=now - timedelta(minutes=1),
        completed_at=now,
    )


class IncomingPipeline(TestCase):
    """staged, idempotent application of deliveries"""

    @classmethod
    def setUpTestData(cls):
        """basic user data"""
        with (
            patch("bookwyrm.suggested_users.rerank_suggestions_task.delay"),
            patch("bookwyrm.activitystreams.populate_stream_task.delay"),
            patch("bookwyrm.lists_stream.populate_lists_task.delay"),
        ):
            cls.local_user = models.User.objects.create_user(
                "mouse@example.com",
                "mouse@mouse.com",
                "mouseword",
                local=True,
                localname="mouse",
            )
        cls.local_user.remote_id = "https://example.com/user/mouse"
        cls.local_user.save(broadcast=False, update_fields=["remote_id"])
        with patch("bookwyrm.models.user.set_remote_server.delay"):
            cls.remote_user = models.User.objects.create_user(
                "rat",
                "rat@rat.com",
                "ratword",
                local=False,
                remote_id="https://example.com/users/rat",
                inbox="https://example.com/users/rat/inbox",
                outbox="https://example.com/users/rat/outbox",
            )

        datafile = pathlib.Path(__file__).parent.joinpath("data/ap_note.json")
        cls.note_data = json.loads(datafile.read_bytes())

        models.SiteSettings.objects.create()

    def _follow(self, activity_id="https://example.com/users/rat/follows/1"):
        return {
            "@context": "https://www.w3.org/ns/activitystreams",
            "id": activity_id,
            "type": "Follow",
            "actor": self.remote_user.remote_id,
            "object": self.local_user.remote_id,
        }

    # -- claim and de-duplication -------------------------------------------

    def test_redelivery_applies_once(self):
        """a second delivery of the same activity does nothing again"""
        activity = self._follow()
        with patch(
            "bookwyrm.models.activitypub_mixin.broadcast_task.apply_async"
        ) as mock:
            incoming.process(activity)
            self.assertEqual(mock.call_count, 1)
            incoming.process(activity)
            self.assertEqual(mock.call_count, 1)

        self.assertEqual(
            models.UserFollows.objects.filter(
                user_subject=self.remote_user, user_object=self.local_user
            ).count(),
            1,
        )
        delivery = models.InboxActivity.objects.get(activity_id=activity["id"])
        self.assertEqual(delivery.result, models.InboxActivity.Result.COMPLETED)
        # the duplicate returned before any stage was replayed
        self.assertEqual(delivery.attempts, 1)

    def test_claim_records_signature_sender_and_digest(self):
        """the ledger claims by activity id, sender and body digest"""
        activity = self._follow()
        delivery = incoming.claim(
            activity,
            signature_key_id="https://example.com/users/rat#main-key",
            body_digest="abc123",
        )
        self.assertEqual(delivery.sender, self.remote_user.remote_id)
        self.assertEqual(
            delivery.signature_key_id, "https://example.com/users/rat#main-key"
        )
        self.assertEqual(delivery.body_digest, "abc123")

        # same payload is the same delivery
        again = incoming.claim(activity, body_digest="abc123")
        self.assertEqual(again.pk, delivery.pk)

        # a different body for the same id is a conflict
        with self.assertRaises(incoming.DeliveryConflict):
            incoming.claim(activity, body_digest="different")

    def test_conflicting_redelivery_rejected_at_view(self):
        """the inbox answers a forged redelivery with 401"""
        client = Client()
        activity = self._follow()
        with (
            patch("bookwyrm.views.inbox.has_valid_signature", return_value=True),
            patch("bookwyrm.models.activitypub_mixin.broadcast_task.apply_async"),
        ):
            result = client.post(
                "/inbox", json.dumps(activity), content_type="application/json"
            )
            self.assertEqual(result.status_code, 200)

            # same activity id, altered payload
            altered = dict(activity)
            altered["object"] = "https://example.com/users/rat"
            result = client.post(
                "/inbox",
                json.dumps(altered),
                content_type="application/json",
            )
            self.assertEqual(result.status_code, 401)

    # -- sync/async share one record -----------------------------------------

    def test_sync_then_async_share_delivery(self):
        """the sync attempt and the async task continue the same record"""
        activity = self._follow()
        # the object is not locally available, so sync processing demurs
        activity["object"] = "https://example.com/users/other"
        with self.assertRaises(ActivitySerializerError):
            incoming.process(activity, allow_external_connections=False)

        self.assertEqual(
            models.InboxActivity.objects.filter(activity_id=activity["id"]).count(),
            1,
        )
        delivery = models.InboxActivity.objects.get(activity_id=activity["id"])
        self.assertEqual(delivery.result, models.InboxActivity.Result.NOT_APPLIED)

        # the async task continues that record and applies once
        def resolved(remote_id, **kwargs):
            if remote_id == self.remote_user.remote_id:
                return self.remote_user
            return self.local_user

        with (
            patch("bookwyrm.models.activitypub_mixin.broadcast_task.apply_async"),
            patch("bookwyrm.activitypub.resolve_remote_id", side_effect=resolved),
        ):
            incoming.process(activity, allow_external_connections=True)

        self.assertEqual(
            models.InboxActivity.objects.filter(activity_id=activity["id"]).count(),
            1,
        )
        self.assertTrue(
            models.UserFollows.objects.filter(
                user_subject=self.remote_user, user_object=self.local_user
            ).exists()
        )
        delivery.refresh_from_db()
        self.assertEqual(delivery.result, models.InboxActivity.Result.COMPLETED)

    # -- remote fetch failures -----------------------------------------------

    def test_fetch_failure_stays_not_applied_and_retries(self):
        """a failed fetch writes nothing and the delivery stays retryable"""
        activity = self._follow()
        activity["object"] = "https://example.com/users/other"

        import requests

        def boom(remote_id, **kwargs):
            if remote_id == self.remote_user.remote_id:
                return self.remote_user
            raise requests.exceptions.ConnectionError("no route to host")

        with patch("bookwyrm.activitypub.resolve_remote_id", side_effect=boom):
            with self.assertRaises(incoming.RecoverableDelivery):
                incoming.process(activity, allow_external_connections=True)

        self.assertFalse(models.UserFollowRequest.objects.exists())
        self.assertFalse(models.UserFollows.objects.exists())
        delivery = models.InboxActivity.objects.get(activity_id=activity["id"])
        self.assertEqual(delivery.result, models.InboxActivity.Result.NOT_APPLIED)
        self.assertEqual(delivery.status, models.InboxActivity.Status.PENDING)
        self.assertTrue(delivery.error)

        # retry, now that the remote is reachable, applies the delivery
        def resolved(remote_id, **kwargs):
            if remote_id == self.remote_user.remote_id:
                return self.remote_user
            return self.local_user

        with (
            patch("bookwyrm.models.activitypub_mixin.broadcast_task.apply_async"),
            patch("bookwyrm.activitypub.resolve_remote_id", side_effect=resolved),
        ):
            incoming.process(activity, allow_external_connections=True)

        delivery.refresh_from_db()
        self.assertEqual(delivery.result, models.InboxActivity.Result.COMPLETED)
        self.assertTrue(models.UserFollows.objects.exists())

    # -- crash after the domain commit --------------------------------------

    def test_resume_completes_notifications_without_changing_objects(self):
        """applied-pending deliveries only supplement, they don't re-apply"""
        # the follow relationship was committed, then the worker died before
        # the Accept broadcast and follow notification were confirmed
        follow = models.UserFollows.objects.create(
            user_subject=self.remote_user,
            user_object=self.local_user,
        )
        activity = self._follow("https://example.com/users/rat/follows/crashed")
        now = timezone.now()
        delivery = incoming.claim(activity)
        delivery.resolved_at = now - timedelta(minutes=2)
        delivery.applied_at = now - timedelta(minutes=1)
        delivery.status = models.InboxActivity.Status.APPLIED
        delivery.save()
        self.assertEqual(delivery.result, models.InboxActivity.Result.APPLIED_PENDING)

        with patch(
            "bookwyrm.models.activitypub_mixin.broadcast_task.apply_async"
        ) as mock:
            incoming.process(activity)

        # no second relationship, but the missing after-effects are present
        self.assertEqual(
            models.UserFollows.objects.filter(
                user_subject=self.remote_user, user_object=self.local_user
            ).count(),
            1,
        )
        self.assertEqual(follow.pk, models.UserFollows.objects.get().pk)
        self.assertEqual(mock.call_count, 1)
        self.assertEqual(
            models.Notification.objects.filter(
                user=self.local_user,
                notification_type=models.NotificationType.FOLLOW,
            ).count(),
            1,
        )
        delivery.refresh_from_db()
        self.assertEqual(delivery.result, models.InboxActivity.Result.COMPLETED)

    def test_resume_creates_missing_mention_notification(self):
        """a committed status gets its mention notification on resume"""
        status = models.Status.objects.create(
            user=self.remote_user,
            content="test content in note",
            remote_id=self.note_data["id"],
        )
        status.mention_users.add(self.local_user)

        activity = {
            "id": "note-create",
            "type": "Create",
            "actor": self.remote_user.remote_id,
            "to": ["https://www.w3.org/ns/activitystreams#public"],
            "object": self.note_data,
        }
        delivery = incoming.claim(activity)
        now = timezone.now()
        delivery.resolved_at = now
        delivery.applied_at = now
        delivery.status = models.InboxActivity.Status.APPLIED
        delivery.save()

        self.assertFalse(
            models.Notification.objects.filter(
                notification_type=models.NotificationType.MENTION
            ).exists()
        )
        with patch("bookwyrm.activitystreams.add_status_task.delay"):
            incoming.process(activity)

        self.assertEqual(models.Status.objects.count(), 1)
        self.assertTrue(
            models.Notification.objects.filter(
                user=self.local_user,
                notification_type=models.NotificationType.MENTION,
            ).exists()
        )
        delivery.refresh_from_db()
        self.assertEqual(delivery.result, models.InboxActivity.Result.COMPLETED)

    # -- generations ----------------------------------------------------------

    def test_late_update_after_delete_is_refused(self):
        """an Update older than a Delete can't resurrect a tombstone"""
        with patch("bookwyrm.activitystreams.remove_status_task.delay"):
            status = models.Status.objects.create(
                user=self.remote_user,
                content="hello",
                remote_id="https://example.com/status/1",
            )
            status.delete()

        new_time = (timezone.now() - timedelta(days=1)).isoformat()
        _completed_delivery(
            "delete-1",
            "Delete",
            "object:https://example.com/status/1",
            new_time,
            target="https://example.com/status/1",
        )

        old_time = (timezone.now() - timedelta(days=2)).isoformat()
        late_update = {
            "id": "late-update",
            "type": "Update",
            "actor": self.remote_user.remote_id,
            "published": old_time,
            "to": [],
            "cc": [],
            "object": {
                "id": "https://example.com/status/1",
                "type": "Note",
                "attributedTo": self.remote_user.remote_id,
                "published": old_time,
                "to": [],
                "cc": [],
                "content": "from the past",
            },
        }
        delivery = incoming.process(late_update, allow_external_connections=True)

        self.assertEqual(delivery.result, models.InboxActivity.Result.REJECTED)
        self.assertEqual(
            delivery.rejection_reason,
            models.InboxActivity.RejectionReason.STALE_GENERATION,
        )
        self.assertTrue(models.Status.objects.get().deleted)

    def test_late_delete_after_newer_update_is_refused(self):
        """a Delete older than a newer Update doesn't remove the object"""
        models.Status.objects.create(
            user=self.remote_user,
            content="hello",
            remote_id="https://example.com/status/2",
        )
        new_time = (timezone.now() - timedelta(days=1)).isoformat()
        _completed_delivery(
            "update-2",
            "Update",
            "object:https://example.com/status/2",
            new_time,
            target="https://example.com/status/2",
        )

        old_time = (timezone.now() - timedelta(days=2)).isoformat()
        late_delete = {
            "id": "late-delete",
            "type": "Delete",
            "actor": self.remote_user.remote_id,
            "published": old_time,
            "to": [],
            "cc": [],
            "object": {
                "id": "https://example.com/status/2",
                "type": "Tombstone",
            },
        }
        with patch("bookwyrm.activitystreams.remove_status_task.delay"):
            delivery = incoming.process(late_delete)

        self.assertEqual(
            delivery.rejection_reason,
            models.InboxActivity.RejectionReason.STALE_GENERATION,
        )
        self.assertFalse(models.Status.objects.get().deleted)

    def test_late_undo_after_newer_follow_is_refused(self):
        """an old Undo(Follow) can't remove a newer follow relationship"""
        models.UserFollows.objects.create(
            user_subject=self.remote_user,
            user_object=self.local_user,
        )
        group = f"follow:{self.remote_user.remote_id}|{self.local_user.remote_id}"
        new_time = (timezone.now() - timedelta(days=1)).isoformat()
        _completed_delivery("follow-new", "Follow", group, new_time)

        old_time = (timezone.now() - timedelta(days=2)).isoformat()
        undo = {
            "id": "late-undo",
            "type": "Undo",
            "actor": self.remote_user.remote_id,
            "published": old_time,
            "object": {
                "id": "follow-old",
                "type": "Follow",
                "actor": self.remote_user.remote_id,
                "object": self.local_user.remote_id,
                "published": old_time,
            },
        }
        delivery = incoming.process(undo)

        self.assertEqual(
            delivery.rejection_reason,
            models.InboxActivity.RejectionReason.STALE_GENERATION,
        )
        self.assertTrue(
            models.UserFollows.objects.filter(
                user_subject=self.remote_user, user_object=self.local_user
            ).exists()
        )

    def test_banned_sender_is_refused_but_self_delete_applies(self):
        """activities from deleted accounts are refused, except their Delete"""
        with patch("bookwyrm.suggested_users.remove_user_task.delay"):
            self.remote_user.delete(broadcast=False)

        follow = self._follow("follow-after-ban")
        delivery = incoming.process(follow)
        self.assertEqual(delivery.result, models.InboxActivity.Result.REJECTED)
        self.assertEqual(
            delivery.rejection_reason,
            models.InboxActivity.RejectionReason.GONE_ACTOR,
        )
        self.assertFalse(models.UserFollows.objects.exists())

        # the Delete that performs the ban is still allowed through
        delete_self = {
            "id": "self-delete",
            "type": "Delete",
            "actor": self.remote_user.remote_id,
            "to": [],
            "cc": [],
            "object": self.remote_user.remote_id,
        }
        delivery = incoming.process(delete_self)
        self.assertEqual(delivery.result, models.InboxActivity.Result.COMPLETED)

    def test_activity_predating_account_migration_is_refused(self):
        """a follow older than a Move must not touch the migrated account"""
        with patch("bookwyrm.models.user.set_remote_server.delay"):
            target = models.User.objects.create_user(
                "rat2",
                "rat2@rat.com",
                "ratword",
                local=False,
                remote_id="https://example.com/users/rat2",
            )
        # the migration is only valid when the source is known to the target
        target.also_known_as.add(self.remote_user)
        with patch("bookwyrm.models.activitypub_mixin.broadcast_task.apply_async"):
            models.MoveUser(
                user=self.remote_user,
                object=self.remote_user.remote_id,
                target=target,
            ).save()
        self.assertEqual(self.remote_user.moved_to, target.remote_id)

        old_follow = self._follow("follow-before-move")
        old_follow["published"] = "2020-01-01T00:00:00Z"
        delivery = incoming.process(old_follow)

        self.assertEqual(
            delivery.rejection_reason,
            models.InboxActivity.RejectionReason.STALE_GENERATION,
        )
        self.assertFalse(models.UserFollows.objects.exists())

    # -- restart recovery -----------------------------------------------------

    def test_recovery_sweep_resumes_abandoned_delivery(self):
        """after a restart, stale deliveries are re-queued and completed"""
        activity = self._follow("abandoned-follow")
        delivery = incoming.claim(activity)
        # the worker died while the delivery was in flight
        models.InboxActivity.objects.filter(pk=delivery.pk).update(
            status=models.InboxActivity.Status.PROCESSING,
            updated_date=timezone.now() - timedelta(minutes=20),
        )

        # a delivery that was accepted seconds ago is not swept
        fresh = self._follow("fresh-follow")
        incoming.claim(fresh)

        with patch("bookwyrm.views.inbox.activity_task.apply_async") as mock:
            enqueued = incoming.recover_stale_deliveries_task()

        self.assertEqual(enqueued, 1)
        self.assertEqual(mock.call_args.kwargs["args"], [activity])

        # the resumed worker completes the delivery from its stored payload
        with patch("bookwyrm.models.activitypub_mixin.broadcast_task.apply_async"):
            incoming.process(activity)
        delivery.refresh_from_db()
        self.assertEqual(delivery.result, models.InboxActivity.Result.COMPLETED)
        self.assertTrue(
            models.UserFollows.objects.filter(
                user_subject=self.remote_user, user_object=self.local_user
            ).exists()
        )
