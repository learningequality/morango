import json
import platform
import uuid
import logging

from django.core.exceptions import ValidationError
from django.utils import timezone
from ipware.ip import get_ip
from rest_framework import mixins
from rest_framework import pagination
from rest_framework import response
from rest_framework import status
from rest_framework import viewsets
from rest_framework.parsers import JSONParser

import morango
from morango import errors
from morango.api import permissions
from morango.api import serializers
from morango.constants import transfer_stages
from morango.constants import transfer_statuses
from morango.constants.capabilities import ASYNC_OPERATIONS
from morango.constants.capabilities import GZIP_BUFFER_POST
from morango.models import certificates
from morango.models.core import Buffer
from morango.models.core import Certificate
from morango.models.core import InstanceIDModel
from morango.models.core import SyncSession
from morango.models.core import TransferSession
from morango.models.fields.crypto import SharedKey
from morango.sync.context import LocalSessionContext
from morango.sync.controller import SessionController
from morango.utils import CAPABILITIES
from morango.utils import _assert
from morango.utils import parse_capabilities_from_server_request


if GZIP_BUFFER_POST in CAPABILITIES:
    from .parsers import GzipParser

    parsers = (GzipParser, JSONParser)
else:
    parsers = (JSONParser,)


def controller_signal_logger(context=None):
    _assert(context is not None, "Missing context")

    if context.stage_status == transfer_statuses.PENDING:
        logging.info("Starting stage '{}'".format(context.stage))
    elif context.stage_status == transfer_statuses.STARTED:
        logging.info("Stage '{}' is in progress".format(context.stage))
    elif context.stage_status == transfer_statuses.COMPLETED:
        logging.info("Completed stage '{}'".format(context.stage))
    elif context.stage_status == transfer_statuses.ERRORED:
        logging.info("Encountered error during stage '{}'".format(context.stage))


session_controller = SessionController.build()
session_controller.signals.connect(controller_signal_logger)


class CertificateChainViewSet(viewsets.ViewSet):
    permissions = (permissions.CertificatePushPermissions,)

    def create(self, request):
        # pop last certificate in chain
        cert_chain = json.loads(request.data)
        client_cert = cert_chain.pop()

        # verify the rest of the cert chain
        try:
            Certificate.save_certificate_chain(cert_chain)
        except (AssertionError, errors.MorangoCertificateError) as e:
            return response.Response(
                "Saving certificate chain has failed: {}".format(str(e)),
                status=status.HTTP_403_FORBIDDEN,
            )

        # create an in-memory instance of the cert from the serialized data and signature
        certificate = Certificate.deserialize(
            client_cert["serialized"], client_cert["signature"]
        )

        # check if certificate's public key is in our list of shared keys
        try:
            sharedkey = SharedKey.objects.get(public_key=certificate.public_key)
        except SharedKey.DoesNotExist:
            return response.Response(
                "Shared public key was not used", status=status.HTTP_400_BAD_REQUEST
            )

        # set private key
        certificate.private_key = sharedkey.private_key

        # check that the nonce is valid, and consume it so it can't be used again
        try:
            certificates.Nonce.use_nonce(certificate.salt)
        except errors.MorangoNonceError:
            return response.Response(
                "Nonce (certificate's salt) is not valid",
                status=status.HTTP_403_FORBIDDEN,
            )

        # verify the certificate (scope is a subset, profiles match, etc)
        try:
            certificate.check_certificate()
        except errors.MorangoCertificateError as e:
            return response.Response(
                {
                    "error_class": e.__class__.__name__,
                    "error_message": getattr(
                        e, "message", (getattr(e, "args") or ("",))[0]
                    ),
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # we got this far, and everything looks good, so we can save the certificate
        certificate.save()

        return response.Response(
            "Certificate chain has been saved", status=status.HTTP_201_CREATED
        )


class CertificateViewSet(
    viewsets.mixins.CreateModelMixin,
    viewsets.mixins.RetrieveModelMixin,
    viewsets.mixins.ListModelMixin,
    viewsets.GenericViewSet,
):
    permission_classes = (permissions.CertificatePermissions,)
    serializer_class = serializers.CertificateSerializer
    authentication_classes = (permissions.BasicMultiArgumentAuthentication,)

    def create(self, request):

        serialized_cert = serializers.CertificateSerializer(data=request.data)

        if serialized_cert.is_valid():

            # inflate the provided data into an actual in-memory certificate
            certificate = Certificate(**serialized_cert.validated_data)

            # add a salt, ID and signature to the certificate
            certificate.salt = uuid.uuid4().hex
            certificate.id = certificate.calculate_uuid()
            certificate.parent.sign_certificate(certificate)

            # ensure that the certificate model fields validate
            try:
                certificate.full_clean()
            except ValidationError as e:
                return response.Response(e, status=status.HTTP_400_BAD_REQUEST)

            # verify the certificate (scope is a subset, profiles match, etc)
            try:
                certificate.check_certificate()
            except errors.MorangoCertificateError as e:
                return response.Response(
                    {
                        "error_class": e.__class__.__name__,
                        "error_message": getattr(
                            e, "message", (getattr(e, "args") or ("",))[0]
                        ),
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # we got this far, and everything looks good, so we can save the certificate
            certificate.save()

            # return a serialized copy of the signed certificate to the client
            return response.Response(
                serializers.CertificateSerializer(certificate).data,
                status=status.HTTP_201_CREATED,
            )

        else:
            return response.Response(
                serialized_cert.errors, status=status.HTTP_400_BAD_REQUEST
            )

    def get_queryset(self):

        params = self.request.query_params

        base_queryset = Certificate.objects

        # filter by profile, if requested
        if "profile" in params:
            base_queryset = base_queryset.filter(profile=params["profile"])

        try:

            # if specified, filter by primary partition, and only include certs the server owns
            if "primary_partition" in params:
                target_cert = base_queryset.get(id=params["primary_partition"])
                return target_cert.get_descendants(include_self=True).exclude(
                    _private_key=None
                )

            # if specified, return the certificate chain for a certificate owned by the server
            if "ancestors_of" in params:
                target_cert = base_queryset.exclude(_private_key=None).get(
                    id=params["ancestors_of"]
                )
                return target_cert.get_ancestors(include_self=True)

        except Certificate.DoesNotExist:
            # if the target_cert can't be found, just return an empty queryset
            return base_queryset.none()

        # if no filters were specified, just return all certificates owned by the server
        return base_queryset.exclude(_private_key=None)


class NonceViewSet(viewsets.mixins.CreateModelMixin, viewsets.GenericViewSet):
    serializer_class = serializers.NonceSerializer

    def create(self, request):
        nonce = certificates.Nonce.objects.create(ip=get_ip(request))

        return response.Response(
            serializers.NonceSerializer(nonce).data, status=status.HTTP_201_CREATED
        )


class SyncSessionViewSet(
    viewsets.mixins.DestroyModelMixin,
    viewsets.mixins.RetrieveModelMixin,
    viewsets.GenericViewSet,
):
    serializer_class = serializers.SyncSessionSerializer

    def create(self, request):

        instance_id, _ = InstanceIDModel.get_or_create_current_instance()

        # verify and save the certificate chain to our cert store
        try:
            Certificate.save_certificate_chain(
                request.data.get("certificate_chain"),
                expected_last_id=request.data.get("client_certificate_id"),
            )
        except (AssertionError, errors.MorangoCertificateError):
            return response.Response(
                "Saving certificate chain has failed", status=status.HTTP_403_FORBIDDEN
            )

        # attempt to load the requested certificates
        try:
            server_cert = Certificate.objects.get(
                id=request.data.get("server_certificate_id")
            )
            client_cert = Certificate.objects.get(
                id=request.data.get("client_certificate_id")
            )
        except Certificate.DoesNotExist:
            return response.Response(
                "Requested certificate does not exist!",
                status=status.HTTP_400_BAD_REQUEST,
            )

        if server_cert.profile != client_cert.profile:
            return response.Response(
                "Certificates must both be associated with the same profile",
                status=status.HTTP_400_BAD_REQUEST,
            )

        # check that the nonce/id were properly signed
        message = "{nonce}:{id}".format(
            nonce=request.data.get("nonce"), id=request.data.get("id")
        )
        if not client_cert.verify(message, request.data["signature"]):
            return response.Response(
                "Client certificate failed to verify signature",
                status=status.HTTP_403_FORBIDDEN,
            )

        # check that the nonce is valid, and consume it so it can't be used again
        try:
            certificates.Nonce.use_nonce(request.data["nonce"])
        except errors.MorangoNonceError:
            return response.Response(
                "Nonce is not valid", status=status.HTTP_403_FORBIDDEN
            )

        # build the data to be used for creation the syncsession
        data = {
            "id": request.data.get("id"),
            "start_timestamp": timezone.now(),
            "last_activity_timestamp": timezone.now(),
            "active": True,
            "is_server": True,
            "client_certificate": client_cert,
            "server_certificate": server_cert,
            "profile": server_cert.profile,
            "connection_kind": "network",
            "connection_path": request.data.get("connection_path"),
            "client_ip": get_ip(request) or "",
            "server_ip": request.data.get("server_ip") or "",
            "client_instance": request.data.get("instance"),
            "server_instance": json.dumps(
                serializers.InstanceIDSerializer(instance_id).data
            ),
        }

        syncsession = SyncSession(**data)
        syncsession.full_clean()
        syncsession.save()

        resp_data = {
            "signature": server_cert.sign(message),
            "server_instance": data["server_instance"],
        }

        return response.Response(resp_data, status=status.HTTP_201_CREATED)

    def perform_destroy(self, syncsession):
        syncsession.active = False
        syncsession.save()

    def get_queryset(self):
        return SyncSession.objects.filter(active=True)


class TransferSessionViewSet(
    viewsets.mixins.RetrieveModelMixin,
    viewsets.mixins.UpdateModelMixin,
    viewsets.mixins.DestroyModelMixin,
    viewsets.GenericViewSet,
):
    serializer_class = serializers.TransferSessionSerializer

    def create(self, request):  # noqa: C901

        # attempt to load the requested syncsession
        try:
            syncsession = SyncSession.objects.filter(active=True).get(
                id=request.data.get("sync_session_id")
            )
        except SyncSession.DoesNotExist:
            return response.Response(
                "Requested syncsession does not exist or is no longer active!",
                status=status.HTTP_400_BAD_REQUEST,
            )

        # a push is to transfer data from client to server; a pull is the inverse
        is_a_push = request.data.get("push")

        # check that the requested filter is within the appropriate certificate scopes
        scope_error_msg = None
        requested_filter = certificates.Filter(request.data.get("filter"))
        server_scope = syncsession.server_certificate.get_scope()
        client_scope = syncsession.client_certificate.get_scope()
        if is_a_push:
            if not requested_filter.is_subset_of(client_scope.write_filter):
                scope_error_msg = "Client certificate scope does not permit pushing for the requested filter."
            if not requested_filter.is_subset_of(server_scope.read_filter):
                scope_error_msg = "Server certificate scope does not permit receiving pushes for the requested filter."
        else:
            if not requested_filter.is_subset_of(client_scope.read_filter):
                scope_error_msg = "Client certificate scope does not permit pulling for the requested filter."
            if not requested_filter.is_subset_of(server_scope.write_filter):
                scope_error_msg = "Server certificate scope does not permit responding to pulls for the requested filter."
        if scope_error_msg:
            return response.Response(scope_error_msg, status=status.HTTP_403_FORBIDDEN)

        context = LocalSessionContext(
            request=request,
            sync_session=syncsession,
            sync_filter=requested_filter,
            is_push=is_a_push,
        )

        # If both client and ourselves allow async, we just return accepted status, and the client
        # should PATCH the transfer_session to the appropriate stage. If not async, we wait until
        # queuing is complete
        to_stage = (
            transfer_stages.INITIALIZING
            if self.async_allowed()
            else transfer_stages.QUEUING
        )
        result = session_controller.proceed_to_and_wait_for(
            to_stage, context=context, max_interval=2
        )

        if result == transfer_statuses.ERRORED:
            if context.error:
                raise context.error

            return response.Response(
                "Failed to initialize session",
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        if result == transfer_statuses.COMPLETED:
            response_status = status.HTTP_201_CREATED
        else:
            response_status = status.HTTP_202_ACCEPTED

        return response.Response(
            self.get_serializer(context.transfer_session).data,
            status=response_status,
        )

    def update(self, request, *args, **kwargs):
        if not kwargs.get("partial", False):
            return response.Response(
                "Only PATCH updates allowed", status=status.HTTP_405_METHOD_NOT_ALLOWED
            )

        update_stage = request.data.pop("transfer_stage", None)
        if update_stage is not None:
            # if client is trying to update `transfer_stage`, then we use the controller to proceed
            # to the stage, but wait for completion if both do not support async
            context = LocalSessionContext(
                request=request,
                transfer_session=self.get_object(),
            )
            # special case for transferring, not to wait since it's a chunked process
            if self.async_allowed() or update_stage == transfer_stages.TRANSFERRING:
                session_controller.proceed_to(update_stage, context=context)
            else:
                session_controller.proceed_to_and_wait_for(
                    update_stage, context=context, max_interval=2
                )

        return super(TransferSessionViewSet, self).update(request, *args, **kwargs)

    def perform_destroy(self, transfer_session):
        context = LocalSessionContext(
            request=self.request,
            transfer_session=transfer_session,
        )
        if self.async_allowed():
            session_controller.proceed_to(transfer_stages.CLEANUP, context=context)
        else:
            result = session_controller.proceed_to_and_wait_for(
                transfer_stages.CLEANUP, context=context, max_interval=2
            )
            # raise an error for synchronous, if status is false
            if result == transfer_statuses.ERRORED:
                if context.error:
                    raise context.error
                else:
                    raise RuntimeError("Cleanup failed")

    def get_queryset(self):
        return TransferSession.objects.filter(active=True)

    def async_allowed(self):
        """
        :return: A boolean if async ops are allowed by client and self
        """
        client_capabilities = parse_capabilities_from_server_request(self.request)
        return (
            ASYNC_OPERATIONS in client_capabilities and ASYNC_OPERATIONS in CAPABILITIES
        )


class BufferViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    permission_classes = (permissions.BufferPermissions,)
    serializer_class = serializers.BufferSerializer
    pagination_class = pagination.LimitOffsetPagination
    parser_classes = parsers

    def create(self, request):
        data = request.data if isinstance(request.data, list) else [request.data]
        # ensure the transfer session allows pushes, and is same across records
        transfer_session = TransferSession.objects.get(id=data[0]["transfer_session"])
        if not transfer_session.push:
            return response.Response(
                "Specified TransferSession does not allow pushes.",
                status=status.HTTP_403_FORBIDDEN,
            )
        if len(set(rec["transfer_session"] for rec in data)) > 1:
            return response.Response(
                "All pushed records must be associated with the same TransferSession.",
                status=status.HTTP_403_FORBIDDEN,
            )

        context = LocalSessionContext(
            request=request, transfer_session=transfer_session
        )
        result = session_controller.proceed_to(
            transfer_stages.TRANSFERRING, context=context
        )

        if result == transfer_statuses.ERRORED:
            if context.error:
                raise context.error
            else:
                response_status = status.HTTP_500_INTERNAL_SERVER_ERROR
        else:
            response_status = status.HTTP_201_CREATED

        return response.Response(status=response_status)

    def get_queryset(self):
        session_id = self.request.query_params["transfer_session_id"]
        return Buffer.objects.filter(transfer_session_id=session_id)


class MorangoInfoViewSet(viewsets.ViewSet):
    def retrieve(self, request, pk=None):
        (id_model, _) = InstanceIDModel.get_or_create_current_instance()
        # include custom instance info as well
        m_info = id_model.instance_info.copy()
        m_info.update({
            "instance_hash": id_model.get_proquint(),
            "instance_id": id_model.id,
            "system_os": platform.system(),
            "version": morango.__version__,
            "capabilities": CAPABILITIES,
        })
        return response.Response(m_info)


class PublicKeyViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (permissions.CertificatePushPermissions,)
    serializer_class = serializers.SharedKeySerializer

    def get_queryset(self):
        return SharedKey.objects.filter(current=True)
