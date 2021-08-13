import json

from django.contrib.auth import authenticate
from django.contrib.auth import get_user_model
from rest_framework import authentication
from rest_framework import exceptions
from rest_framework import permissions

from morango.models.core import TransferSession
from morango.utils import SETTINGS


class BasicMultiArgumentAuthentication(authentication.BasicAuthentication):
    """
    HTTP Basic authentication against username (plus any other optional arguments) and password.
    """

    def authenticate_credentials(self, userargs, password, request=None):
        """
        Authenticate the userargs and password against Django auth backends.
        The "userargs" string may be just the username, or a querystring-encoded set of params.
        """

        credentials = {"password": password}

        if "=" not in userargs:
            # if it doesn't seem to be in querystring format, just use it as the username
            credentials[get_user_model().USERNAME_FIELD] = userargs
        else:
            # parse out the user args from querystring format into the credentials dict
            for arg in userargs.split("&"):
                key, val = arg.split("=")
                credentials[key] = val

        # authenticate the user via Django's auth backends
        user = authenticate(request=request, **credentials)

        if user is None:
            raise exceptions.AuthenticationFailed("Invalid credentials.")

        if not user.is_active:
            raise exceptions.AuthenticationFailed("User inactive or deleted.")

        return (user, None)


class CertificatePermissions(permissions.BasePermission):
    def has_permission(self, request, view):

        # the Django REST Framework browseable API calls this to see what buttons to show
        if not request.data:
            return True

        # we allow anyone to read certificates
        if request.method in permissions.SAFE_METHODS:
            return True

        # other than read (or other safe) operations, we only allow POST
        if request.method == "POST":
            # check that the authenticated user has the appropriate permissions to create the certificate
            if hasattr(request.user, "has_morango_certificate_scope_permission"):
                scope_definition_id = request.data.get("scope_definition")
                scope_params = json.loads(request.data.get("scope_params"))
                if (
                    scope_definition_id
                    and scope_params
                    and isinstance(scope_params, dict)
                ):
                    return request.user.has_morango_certificate_scope_permission(
                        scope_definition_id, scope_params
                    )
            return False

        return False


class CertificatePushPermissions(permissions.BasePermission):
    message = "Server does not allow certificate pushing."

    def has_permission(self, request, view):
        if SETTINGS.ALLOW_CERTIFICATE_PUSHING:
            return True
        return False


class BufferPermissions(permissions.BasePermission):
    def has_permission(self, request, view):
        if request.method == "GET":
            sesh_id = request.query_params.get("transfer_session_id")
            if not sesh_id:
                return False
            if not TransferSession.objects.filter(
                id=sesh_id, active=True, push=False
            ).exists():
                return False
            return True

        return request.method in ("POST",)
