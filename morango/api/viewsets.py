import json
import uuid

from django.core.exceptions import ValidationError
from django.http import Http404
from django.utils import timezone
from ipware.ip import get_ip
from rest_framework import viewsets, response, status

from . import serializers, permissions
from .. import models, errors


class CertificateViewSet(viewsets.ModelViewSet):
    permission_classes = (permissions.CertificatePermissions,)
    serializer_class = serializers.CertificateSerializer
    authentication_classes = (permissions.BasicMultiArgumentAuthentication,)

    def create(self, request):

        serialized_cert = serializers.CertificateSerializer(data=request.data)

        if serialized_cert.is_valid():

            # inflate the provided data into an actual in-memory certificate
            certificate = models.Certificate(**serialized_cert.validated_data)

            # add a salt, ID and signature to the certificate
            certificate.salt = uuid.uuid4().hex
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

        base_queryset = models.Certificate.objects

        # filter by profile, if requested
        if "profile" in params:
            base_queryset = base_queryset.filter(profile=params["profile"])

        try:

            # if specified, filter by primary partition, and only include certs the server owns
            if "primary_partition" in params:
                target_cert = base_queryset.get(id=params["primary_partition"])
                return target_cert.get_descendants(include_self=True).exclude(_private_key=None)

            # if specified, return the certificate chain for a certificate owned by the server
            if "ancestors_of" in params:
                target_cert = base_queryset.exclude(_private_key=None).get(id=params["ancestors_of"])
                return target_cert.get_ancestors(include_self=True)

        except models.Certificate.DoesNotExist:
            # if the target_cert can't be found, just return an empty queryset
            return base_queryset.none()

        # if no filters were specified, just return all certificates owned by the server
        return base_queryset.exclude(_private_key=None)


class NonceViewSet(viewsets.ModelViewSet):
    permission_classes = (permissions.NoncePermissions,)
    serializer_class = serializers.NonceSerializer

    def create(self, request):
        nonce = models.Nonce.objects.create(ip=get_ip(request))

        return response.Response(
            serializers.NonceSerializer(nonce).data,
            status=status.HTTP_201_CREATED,
        )


class SyncSessionViewSet(viewsets.ModelViewSet):
    permission_classes = (permissions.SyncSessionPermissions,)
    serializer_class = serializers.SyncSessionSerializer

    def create(self, request):

        instance_id, _ = models.InstanceIDModel.get_or_create_current_instance()

        # attempt to extract the local IP from the 
        local_ip = request.META.get('SERVER_NAME', '')
        try:
            local_ip = socket.gethostbyname(local_ip)
        except:
            pass

        data = {
            "id": request.data.get("id"),
            "start_timestamp": timezone.now(),
            "last_activity_timestamp": timezone.now(),
            "active": True,
            "is_server": True,
            "local_certificate_id": request.data.get("server_certificate_id"),
            "remote_certificate_id": request.data.get("client_certificate_id"),
            "connection_kind": "network",
            "connection_path": request.data.get("connection_path"),
            "local_ip": local_ip,
            "remote_ip": get_ip(request),
            "local_instance": json.dumps(serializers.InstanceIDSerializer(instance_id).data),
            "remote_instance": request.data.get("instance"),
        }

        syncsession = models.SyncSession(**data)
        syncsession.save()
        
        return response.Response(
            serializers.SyncSessionSerializer(syncsession).data,
            status=status.HTTP_201_CREATED,
        )

    def perform_destroy(self, syncsession):
        
        if not syncsession.active:
            raise Http404

        syncsession.active = False
        syncsession.save()
