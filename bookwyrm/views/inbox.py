"""incoming activities"""

import hashlib
import json
import re
import logging

import requests

from django.http import HttpResponse, HttpResponseForbidden, Http404
from django.core.exceptions import BadRequest, PermissionDenied
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt

from bookwyrm import activitypub, incoming, models
from bookwyrm.decorators import require_federation
from bookwyrm.tasks import app, INBOX
from bookwyrm.signatures import Signature
from bookwyrm.utils import regex

logger = logging.getLogger(__name__)


class UserIsGoneError(Exception):
    """error class for when a user is banned or deleted"""


@method_decorator(csrf_exempt, name="dispatch")
@method_decorator(require_federation, name="dispatch")
class Inbox(View):
    """requests sent by outside servers"""

    def post(self, request, username=None):
        """only works as POST request"""
        # first check if this server is on our shitlist
        raise_is_blocked_user_agent(request)

        # make sure the user's inbox even exists
        if username:
            get_object_or_404(models.User, localname=username, is_active=True)

        # is it valid json? does it at least vaguely resemble an activity?
        try:
            activity_json = json.loads(request.body)
        except json.decoder.JSONDecodeError:
            raise BadRequest()

        try:
            # let's be extra sure we didn't block this domain
            raise_is_blocked_activity(activity_json)
        except UserIsGoneError:
            # banned or deleted users are not allowed to send us Activities
            return HttpResponseForbidden()

        if (
            "object" not in activity_json
            or "type" not in activity_json
            or activity_json["type"] not in activitypub.activity_objects
        ):
            raise Http404()

        # verify the signature
        signature = has_valid_signature(request, activity_json)
        if not signature:
            if activity_json["type"] == "Delete":
                # Pretend that unauth'd deletes succeed. Auth may be failing
                # because the resource or owner of the resource might have
                # been deleted.
                return HttpResponse()
            return HttpResponse(status=401)

        # claim the delivery once, now that the signature and block checks
        # passed, so redeliveries return the original acceptance result
        try:
            delivery = incoming.claim(
                activity_json,
                signature_key_id=getattr(signature, "key_id", "") or "",
                body_digest=hashlib.sha256(request.body).hexdigest(),
            )
            sometimes_async_activity_task(activity_json, delivery=delivery)
        except incoming.DeliveryConflict:
            # same activity id, different sender or body: treat as a forgery
            return HttpResponse(status=401)
        return HttpResponse()


def raise_is_blocked_user_agent(request):
    """check if a request is from a blocked server based on user agent"""
    # check user agent
    user_agent = request.headers.get("User-Agent")
    if not user_agent:
        return
    url = re.search(rf"https?://{regex.DOMAIN}/?", user_agent)
    if not url:
        return
    url = url.group()
    if models.FederatedServer.is_blocked(url):
        logger.debug("%s is blocked, denying request based on user agent", url)
        raise PermissionDenied()


def raise_is_blocked_activity(activity_json):
    """get the sender out of activity json and check if it's blocked"""
    actor = activity_json.get("actor")

    if not actor:
        # well I guess it's not even a valid activity so who knows
        return

    # check if the user is banned/deleted in our database
    existing = models.User.find_existing_by_remote_id(actor)
    if existing and existing.deleted:
        logger.debug("%s is banned/deleted, denying request based on actor", actor)
        raise UserIsGoneError()

    # check if we have blocked the whole server
    if models.FederatedServer.is_blocked(actor):
        logger.debug("%s is blocked, denying request based on actor", actor)
        raise PermissionDenied()


def sometimes_async_activity_task(activity_json, delivery=None):
    """Sometimes we can effectively respond to a request without queuing a new task,
    and whenever that is possible, we should do it. Both paths share the same
    delivery record, so the processing mode can't duplicate the application."""
    # try resolving this activity without making any http requests
    try:
        incoming.process(
            activity_json,
            allow_external_connections=False,
            delivery=delivery,
        )
    except activitypub.ActivitySerializerError:
        # if that doesn't work, run it asynchronously. Only the first attempt
        # gets to queue; redeliveries don't stack up duplicate tasks.
        if delivery is None or delivery.mark_queued():
            activity_task.apply_async(args=(activity_json,))


@app.task(bind=True, queue=INBOX, max_retries=6)
def activity_task(self, activity_json):
    """do something with this json we think is legit, resuming the delivery
    at whichever stage is missing"""
    try:
        incoming.process(activity_json, allow_external_connections=True)
    except incoming.RecoverableDelivery as err:
        # remote fetch or similar transient failure: the delivery is still
        # not applied (or only applied pending supplementation), retry it
        raise self.retry(exc=err, countdown=err.countdown, max_retries=6)


def has_valid_signature(request, activity):
    """verify incoming signature. Returns the verified signature, or False"""
    try:
        signature = Signature.parse(request)
        remote_user = activitypub.resolve_remote_id(
            activity.get("actor"), model=models.User, save=False
        )  # don't save the actor unless we actually need to import them
        if not remote_user:
            return False

        if signature.key_id != remote_user.key_pair.remote_id:
            if (
                signature.key_id != f"{remote_user.remote_id}#main-key"
            ):  # legacy Bookwyrm
                raise ValueError("Wrong actor created signature.")

        try:
            signature.verify(remote_user.key_pair.public_key, request)
        except ValueError:
            old_key = remote_user.key_pair.public_key
            remote_user = activitypub.resolve_remote_id(
                remote_user.remote_id, model=models.User, refresh=True
            )
            if remote_user.key_pair.public_key == old_key:
                raise  # Key unchanged.
            signature.verify(remote_user.key_pair.public_key, request)
    except (ValueError, requests.exceptions.HTTPError):
        return False
    return signature
