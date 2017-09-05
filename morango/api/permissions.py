import json

from django.contrib.auth import authenticate
from rest_framework import permissions, authentication, exceptions

from ..crypto import Key
from ..models import Certificate, Nonce, TransferSession, Buffer
from ..errors import MorangoCertificateError, MorangoNonceError


class BasicMultiArgumentAuthentication(authentication.BasicAuthentication):
    """
    HTTP Basic authentication against username (plus any other optional arguments) and password.
    """

    def authenticate_credentials(self, userargs, password):
        """
        Authenticate the userargs and password against Django auth backends.
        The "userargs" string may be just the username, or a querystring-encoded set of params.
        """

        credentials = {
            'password': password
        }

        if "=" not in userargs:
            # if it doesn't seem to be in querystring format, just use it as the username
            credentials[get_user_model().USERNAME_FIELD] = userargs
        else:
            # parse out the user args from querystring format into the credentials dict
            for arg in userargs.split("&"):
                key, val = arg.split("=")
                credentials[key] = val

        # authenticate the user via Django's auth backends
        user = authenticate(**credentials)

        if user is None:
            raise exceptions.AuthenticationFailed('Invalid credentials.')

        if not user.is_active:
            raise exceptions.AuthenticationFailed('User inactive or deleted.')

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
                if scope_definition_id and scope_params and isinstance(scope_params, dict):
                    return request.user.has_morango_certificate_scope_permission(scope_definition_id, scope_params)
            return False

        return False


class NoncePermissions(permissions.BasePermission):

    def has_permission(self, request, view):

        if request.method != "POST":
            return False

        return True


class SyncSessionPermissions(permissions.BasePermission):

    def has_permission(self, request, view):

        if request.method == "DELETE":
            return True

        if request.method == "POST":
            
            # verify and save the certificate chain to our cert store
            try:
                client_cert = Certificate.save_certificate_chain(
                    request.data.pop("certificate_chain"),
                    expected_last_id=request.data.get("client_certificate_id")
                )
            except (AssertionError, MorangoCertificateError):
                return False

            # check that the nonce/id were properly signed
            message = "{nonce}:{id}".format(**request.data)
            if not client_cert.verify(message, request.data.pop("signature")):
                return False

            # check that the nonce is valid, and consume it so it can't be used again
            try:
                Nonce.use_nonce(request.data.pop("nonce"))
            except MorangoNonceError:
                return False

            return True

        return False


class TransferSessionPermissions(permissions.BasePermission):

    def has_permission(self, request, view):

        if request.method == "DELETE":
            return True

        if request.method == "POST":
            return True  # we'll be doing some additional permission checks in the viewset

        return False


class BufferPermissions(permissions.BasePermission):

    def has_permission(self, request, view):

        if request.method == "POST":
            return True

        if request.method == "GET":
            sesh_id = request.query_params.get("transfer_session_id")
            if not sesh_id:
                return False
            if not TransferSession.objects.filter(id=sesh_id, active=True, incoming=False).exists():
                return False
            return True

        return False