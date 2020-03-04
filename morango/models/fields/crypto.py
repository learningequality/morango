"""
We have three types of cryptographic key algorithms to be used: ``rsa``, ``M2Crypto``, and ``Cryptography`` (Ordered for
desirability/efficiency from left to right). We have a base ``Key`` class which uses one of the mentioned key algorithms under the hood.
``Key`` has methods for signing messages using a private key and verifying signed messages using a public key.
``Key`` classes are used for signing/verifying certificates that give various permissions.
"""
import hashlib
import re
import sys

import rsa as PYRSA
from django.db import models
from django.db import transaction

try:
    from M2Crypto import RSA as M2RSA
    from M2Crypto import BIO as M2BIO

    M2CRYPTO_EXISTS = True
except ImportError:
    M2CRYPTO_EXISTS = False

try:
    from cryptography.hazmat.backends import default_backend
    from cryptography import exceptions as crypto_exceptions

    crypto_backend = default_backend()
    from cryptography.hazmat.primitives.asymmetric import (
        rsa as crypto_rsa,
        padding as crypto_padding,
    )
    from cryptography.hazmat.primitives import (
        serialization as crypto_serialization,
        hashes as crypto_hashes,
    )

    # Ignore cryptography versions that do not support the 'sign' method
    if not hasattr(crypto_rsa.RSAPrivateKey, "sign"):
        raise ImportError
    CRYPTOGRAPHY_EXISTS = True
except ImportError:
    CRYPTOGRAPHY_EXISTS = False

if sys.version_info[0] < 3:
    from base64 import encodestring as b64encode, decodestring as b64decode
else:
    from base64 import encodebytes as b64encode, decodebytes as b64decode


PKCS8_HEADER = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A"


class BaseKey(object):
    def __init__(self, private_key_string=None, public_key_string=None):

        if private_key_string:
            self.set_private_key_string(private_key_string)

        if public_key_string:
            self.set_public_key_string(public_key_string)

        # if no keys were provided, assume we're generating a new key
        if not private_key_string and not public_key_string:
            self.generate_new_key(keysize=2048)

    def sign(self, message):

        if not self._private_key:
            raise Exception(
                "Key object does not have a private key defined, and thus cannot be used to sign."
            )

        message = self.ensure_bytes(message)

        signature = self._sign(message)

        return b64encode(signature).decode().replace("\n", "")

    def verify(self, message, signature):

        # assume we're dealing with a base64 encoded signature
        signature = b64decode(signature.encode())

        message = self.ensure_bytes(message)

        # ensure we have a public key we can use use to verify
        if not self._public_key:
            raise Exception(
                "Key object does not have public key defined, and thus cannot be used to verify."
            )

        return self._verify(message, signature)

    def get_public_key_string(self):

        if not self._public_key:
            raise Exception("Key object does not have a public key defined.")

        pem_string = self.ensure_unicode(self._get_public_key_string())

        # remove the headers and footer (to save space, but mostly because the text in them varies)
        pem_string = self._remove_pem_headers(pem_string)

        # remove the PKCS#8 header so the key won't cause problems for older versions
        if pem_string.startswith(PKCS8_HEADER):
            pem_string = pem_string[len(PKCS8_HEADER) :]

        # remove newlines, to ensure consistency
        pem_string = pem_string.replace("\n", "")

        return pem_string

    def get_private_key_string(self):
        if not self._private_key:
            raise Exception("Key object does not have a private key defined.")
        return self.ensure_unicode(self._get_private_key_string())

    def set_public_key_string(self, public_key_string):

        # remove the PEM header/footer
        public_key_string = self._remove_pem_headers(public_key_string)

        self._set_public_key_string(public_key_string)

    def set_private_key_string(self, private_key_string):

        private_key_string = self.ensure_unicode(private_key_string)

        private_key_string = self._add_pem_headers(
            private_key_string, "RSA PRIVATE KEY"
        )

        self._set_private_key_string(private_key_string)

    def _remove_pem_headers(self, pem_string):
        if not pem_string.strip().startswith("-----"):
            return pem_string
        return "\n".join(
            [
                line
                for line in pem_string.split("\n")
                if line and not line.startswith("---")
            ]
        )

    def _add_pem_headers(self, pem_string, header_string):
        context = {
            "key": self._remove_pem_headers(pem_string),
            "header_string": header_string,
        }
        return (
            "-----BEGIN %(header_string)s-----\n%(key)s\n-----END %(header_string)s-----"
            % context
        )

    def ensure_bytes(self, message):
        try:
            return message.encode("utf-8", "replace")
        except (UnicodeDecodeError, TypeError, AttributeError):
            return message

    def ensure_unicode(self, message):
        try:
            return message.decode("utf-8", "replace")
        except (UnicodeDecodeError, TypeError, AttributeError):
            return message

    def __str__(self):
        return self.get_public_key_string()


class PythonRSAKey(BaseKey):

    _public_key = None
    _private_key = None

    def generate_new_key(self, keysize=2048):
        try:
            self._public_key, self._private_key = PYRSA.newkeys(keysize, poolsize=4)
        except:  # noqa: E722
            self._public_key, self._private_key = PYRSA.newkeys(keysize)

    def _sign(self, message):

        return PYRSA.sign(message, self._private_key, "SHA-256")

    def _verify(self, message, signature):

        try:
            PYRSA.verify(message, signature, self._public_key)
            return True
        except PYRSA.pkcs1.VerificationError:
            return False

    def _get_public_key_string(self):
        return self._public_key.save_pkcs1()

    def _get_private_key_string(self):
        return self._private_key.save_pkcs1()

    def _set_public_key_string(self, public_key_string):

        # remove PKCS#8 header if it exists
        if public_key_string.startswith(PKCS8_HEADER):
            public_key_string = public_key_string[len(PKCS8_HEADER) :]

        # add the appropriate PEM header/footer
        public_key_string = self._add_pem_headers(public_key_string, "RSA PUBLIC KEY")

        self._public_key = PYRSA.PublicKey.load_pkcs1(public_key_string)

    def _set_private_key_string(self, private_key_string):
        self._private_key = PYRSA.PrivateKey.load_pkcs1(private_key_string)
        self._public_key = PYRSA.PublicKey(self._private_key.n, self._private_key.e)


class M2CryptoKey(BaseKey):

    _public_key = None
    _private_key = None

    def generate_new_key(self, keysize=2048):
        self._private_key = M2RSA.gen_key(keysize, 65537, lambda x, y, z: None)
        self._public_key = M2RSA.RSA_pub(self._private_key.rsa)

    def _sign(self, message):
        return self._private_key.sign(hashlib.sha256(message).digest(), algo="sha256")

    def _verify(self, message, signature):

        try:
            self._public_key.verify(
                hashlib.sha256(message).digest(), signature, algo="sha256"
            )
            return True
        except M2RSA.RSAError:
            return False

    def _get_public_key_string(self):
        return self._public_key.as_pem(None)

    def _get_private_key_string(self):
        return self._private_key.as_pem(None)

    def _set_public_key_string(self, public_key_string):

        # add the PKCS#8 header if it doesn't exist
        if not public_key_string.startswith(PKCS8_HEADER):
            public_key_string = PKCS8_HEADER + public_key_string

        # break up the base64 key string into lines of max length 64, to please m2crypto
        public_key_string = public_key_string.replace("\n", "")
        public_key_string = "\n".join(re.findall(".{1,64}", public_key_string))

        # add the appropriate PEM header/footer
        public_key_string = self._add_pem_headers(public_key_string, "PUBLIC KEY")

        self._public_key = M2RSA.load_pub_key_bio(
            M2BIO.MemoryBuffer(self.ensure_bytes(public_key_string))
        )

    def _set_private_key_string(self, private_key_string):
        self._private_key = M2RSA.load_key_string(self.ensure_bytes(private_key_string))
        self._public_key = M2RSA.RSA_pub(self._private_key.rsa)


class CryptographyKey(BaseKey):

    _public_key = None
    _private_key = None

    def generate_new_key(self, keysize=2048):
        self._private_key = crypto_rsa.generate_private_key(
            public_exponent=65537, key_size=keysize, backend=crypto_backend
        )
        self._public_key = self._private_key.public_key()

    def _sign(self, message):
        return self._private_key.sign(
            message, crypto_padding.PKCS1v15(), crypto_hashes.SHA256()
        )

    def _verify(self, message, signature):
        try:
            self._public_key.verify(
                signature, message, crypto_padding.PKCS1v15(), crypto_hashes.SHA256()
            )
            return True
        except crypto_exceptions.InvalidSignature:
            return False

    def _get_public_key_string(self):
        return self._public_key.public_bytes(
            encoding=crypto_serialization.Encoding.PEM,
            format=crypto_serialization.PublicFormat.PKCS1,
        )

    def _get_private_key_string(self):
        return self._private_key.private_bytes(
            encoding=crypto_serialization.Encoding.PEM,
            format=crypto_serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=crypto_serialization.NoEncryption(),
        )

    def _set_public_key_string(self, public_key_string):

        # add the PKCS#8 header if it doesn't exist
        if not public_key_string.startswith(PKCS8_HEADER):
            public_key_string = PKCS8_HEADER + public_key_string

        # break up the base64 key string into lines of max length 64, to please cryptography
        public_key_string = public_key_string.replace("\n", "")
        public_key_string = "\n".join(re.findall(".{1,64}", public_key_string))

        # add the appropriate PEM header/footer
        public_key_string = self._add_pem_headers(public_key_string, "PUBLIC KEY")

        self._public_key = crypto_serialization.load_pem_public_key(
            self.ensure_bytes(public_key_string), backend=crypto_backend
        )

    def _set_private_key_string(self, private_key_string):

        self._private_key = crypto_serialization.load_pem_private_key(
            self.ensure_bytes(private_key_string), password=None, backend=crypto_backend
        )
        self._public_key = self._private_key.public_key()


# alias the most-preferred key wrapper class we have available as `Key`
Key = (
    CryptographyKey
    if CRYPTOGRAPHY_EXISTS
    else (M2CryptoKey if M2CRYPTO_EXISTS else PythonRSAKey)
)


class RSAKeyBaseField(models.TextField):
    def __init__(self, *args, **kwargs):
        kwargs["max_length"] = 1000
        super(RSAKeyBaseField, self).__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super(RSAKeyBaseField, self).deconstruct()
        del kwargs["max_length"]
        return name, path, args, kwargs


class PublicKeyField(RSAKeyBaseField):
    def from_db_value(self, value, expression, connection, context):
        if not value:
            return None
        return Key(public_key_string=value)

    def to_python(self, value):
        if not value:
            return None
        if isinstance(value, Key):
            return value
        return Key(public_key_string=value)

    def get_prep_value(self, value):
        if not value:
            return None
        return value.get_public_key_string()


class PrivateKeyField(RSAKeyBaseField):
    def from_db_value(self, value, expression, connection, context):
        if not value:
            return None
        return Key(private_key_string=value)

    def to_python(self, value):
        if not value:
            return None
        if isinstance(value, Key):
            return value
        return Key(private_key_string=value)

    def get_prep_value(self, value):
        if not value:
            return None
        return value.get_private_key_string()


class SharedKey(models.Model):
    """
    The public key is publically available via the ``api/morango/v1/publickey`` endpoint. Applications
    who would like to allow certificates to be pushed to the server must also enable ``ALLOW_CERTIFICATE_PUSHING``.
    Clients generate a ``Certificate`` object and set the ``public_key`` field to the shared public key of the server.
    """
    public_key = PublicKeyField()
    private_key = PrivateKeyField()
    current = models.BooleanField(default=True)

    @classmethod
    def get_or_create_shared_key(cls, force_new=False):
        """
        Create a shared public/private key pair for certificate pushing,
        if the settings allow.
        """
        if force_new:
            with transaction.atomic():
                SharedKey.objects.filter(current=True).update(current=False)
                key = Key()
                return SharedKey.objects.create(
                    public_key=key, private_key=key, current=True
                )
        # create a new shared key if one doesn't exist
        try:
            return SharedKey.objects.get(current=True)
        except SharedKey.DoesNotExist:
            key = Key()
            return SharedKey.objects.create(
                public_key=key, private_key=key, current=True
            )
