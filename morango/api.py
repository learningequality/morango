import json

from django.contrib.auth import authenticate
from django.core.exceptions import ValidationError
from rest_framework import filters, pagination, viewsets, permissions, response, status, authentication

from . import models, serializers, errors


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
            raise exceptions.AuthenticationFailed(_('Invalid credentials.'))

        if not user.is_active:
            raise exceptions.AuthenticationFailed(_('User inactive or deleted.'))

        return (user, None)


class CertificatePermissions(permissions.BasePermission):
    """
    Object-level permission to only allow owners of an object to edit it.
    Assumes the model instance has an `owner` attribute.
    """

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


class CertificateViewSet(viewsets.ModelViewSet):
    permission_classes = (CertificatePermissions,)
    serializer_class = serializers.CertificateSerializer
    authentication_classes = (authentication.SessionAuthentication, BasicMultiArgumentAuthentication)

    def create(self, request):

        serialized_cert = serializers.CertificateSerializer(data=request.data)

        if serialized_cert.is_valid():

            # inflate the provided data into an actual in-memory certificate
            certificate = models.Certificate(**serialized_cert.validated_data)

            # add an ID and signature to the certificate
            certificate.id = certificate.calculate_uuid()
            certificate.parent.sign_certificate(certificate)

            # ensure that the certificate model fields validate
            try:
                certificate.full_clean()
            except ValidationError as e:
                return response.Response(
                    e,
                    status=status.HTTP_400_BAD_REQUEST
                )

            # verify the certificate (scope is a subset, profiles match, etc)
            try:
                certificate.check_certificate()
            except errors.MorangoCertificateError as e:
                return response.Response(
                    {e.__class__.__name__: e.message},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # we got this far, and everything looks good, so we can save the certificate
            # TODO: How do we prevent someone from creating a fake cert (for a public key
            # they don't own but know about from their own syncing)? Require a signed nonce
            # as part of the POST here?
            certificate.save()

            # return a serialized copy of the signed certificate to the client
            return response.Response(
                serializers.CertificateSerializer(certificate).data,
                status=status.HTTP_201_CREATED
            )

        else:
            # error = serialized_cert.errors.get('username', None)
            return response.Response(serialized_cert.errors, status=status.HTTP_400_BAD_REQUEST)

    def get_queryset(self):

        params = self.request.query_params

        try:

            # if specified, filter by primary partition, and only include certs the server owns
            if "primary_partition" in params:
                target_cert = models.Certificate.objects.get(id=params["primary_partition"])
                return target_cert.get_descendants(include_self=True).exclude(_private_key=None)

            # if specified, return the certificate chain for a certificate owned by the server
            if "ancestors_of" in params:
                target_cert = models.Certificate.objects.exclude(_private_key=None).get(id=params["ancestors_of"])
                return target_cert.get_ancestors(include_self=True)

        except models.Certificate.DoesNotExist:
            # if the target_cert can't be found, just return an empty queryset
            return models.Certificate.objects.none()

        # TODO: only allow this full list view if the user is authenticated / superuser?
        return models.Certificate.objects.all()
