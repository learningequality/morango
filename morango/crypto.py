import base64, hashlib, sys, re
import rsa as PYRSA

from django.db import models

try:
    from M2Crypto import RSA as M2RSA
    from M2Crypto import BIO as M2BIO
    M2CRYPTO_EXISTS = True
except:
    M2CRYPTO_EXISTS = False

PKCS8_HEADER = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A"

class Key(object):
    
    _public_key = None
    _private_key = None
    _using_m2crypto = False
    
    def __init__(self, private_key_string=None, public_key_string=None, use_m2crypto=M2CRYPTO_EXISTS):
        
        self._using_m2crypto = use_m2crypto
                
        if private_key_string:
            self.set_private_key_string(private_key_string)
        
        if public_key_string:
            self.set_public_key_string(public_key_string)
        
        # if no keys were provided, assume we're generating a new key
        if not private_key_string and not public_key_string:
            self._generate_new_key()

    def _generate_new_key(self, keysize=2048):
        if self._using_m2crypto:
            self._private_key = M2RSA.gen_key(keysize, 65537, lambda x,y,z: None)
            self._public_key = M2RSA.RSA_pub(self._private_key.rsa)
        else:
            try:
                (self._public_key, self._private_key) = PYRSA.newkeys(keysize, poolsize=4)
            except:
                (self._public_key, self._private_key) = PYRSA.newkeys(keysize)
    
    def sign(self, message, base64encode=True):
        
        if not self._private_key:
            raise Exception("Key object does not have a private key defined, and thus cannot be used to sign.")
        
        if self._using_m2crypto:
            signature = self._private_key.sign(hashed(message), algo="sha256")
        else:
            signature = PYRSA.sign(message, self._private_key, "SHA-256")
        
        if base64encode:
            return base64.encodestring(signature).replace("\n", "")
        else:
            return signature
            
    def verify(self, message, signature):
        # assume we're dealing with a base64 encoded signature, but pass on through if not
        try:
            signature = base64.decodestring(signature)
        except:
            pass
        
        # try verifying using the public key if available, otherwise the private key
        key = self._public_key or self._private_key or None
        if not key:
            raise Exception("Key object does not have public or private key defined, and thus cannot be used to verify.")
        
        if self._using_m2crypto:
            try:
                key.verify(hashed(message), signature, algo="sha256")
                return True
            except M2RSA.RSAError:
                return False
        else:
            try:
                PYRSA.verify(message, signature, key)
                return True
            except PYRSA.pkcs1.VerificationError:
                return False

    def get_public_key_string(self):
        
        if not self._public_key:
            raise Exception("Key object does not have a public key defined.")
        
        if self._using_m2crypto:
            pem_string = self._public_key.as_pem(None)
        else:
            pem_string = self._public_key.save_pkcs1()
        
        # remove the headers and footer (to save space, but mostly because the text in them varies)
        pem_string = self._remove_pem_headers(pem_string)
        
        # remove the PKCS#8 header so the key won't cause problems for older versions
        if pem_string.startswith(PKCS8_HEADER):
            pem_string = pem_string[len(PKCS8_HEADER):]
        
        # remove newlines, to ensure consistency
        pem_string = pem_string.replace("\n", "")
        
        return pem_string

    def get_private_key_string(self):
        if not self._private_key:
            raise Exception("Key object does not have a private key defined.")
        if self._using_m2crypto:
            return self._private_key.as_pem(None)
        else:
            return self._private_key.save_pkcs1()

    def set_public_key_string(self, public_key_string):
        
        # convert from unicode, as this can throw off the key parsing
        public_key_string = str(public_key_string)
        
        # remove the PEM header/footer
        public_key_string = self._remove_pem_headers(public_key_string)
                
        if self._using_m2crypto:
            header_string = "PUBLIC KEY"
            # add the PKCS#8 header if it doesn't exist
            if not public_key_string.startswith(PKCS8_HEADER):
                public_key_string = PKCS8_HEADER + public_key_string
            # break up the base64 key string into lines of max length 64, to please m2crypto
            public_key_string = public_key_string.replace("\n", "")
            public_key_string = "\n".join(re.findall(".{1,64}", public_key_string))
        else:
            header_string = "RSA PUBLIC KEY"
            # remove PKCS#8 header if it exists
            if public_key_string.startswith(PKCS8_HEADER):
                public_key_string = public_key_string[len(PKCS8_HEADER):]

        # add the appropriate PEM header/footer
        public_key_string = self._add_pem_headers(public_key_string, header_string)
        
        if self._using_m2crypto:
            self._public_key = M2RSA.load_pub_key_bio(M2BIO.MemoryBuffer(public_key_string))
        else:
            self._public_key = PYRSA.PublicKey.load_pkcs1(public_key_string)

    def set_private_key_string(self, private_key_string):

        # convert from unicode, as this can throw off the key parsing
        private_key_string = str(private_key_string)

        private_key_string = self._add_pem_headers(private_key_string, "RSA PRIVATE KEY")
        
        if self._using_m2crypto:
            self._private_key = M2RSA.load_key_string(private_key_string)
            self._public_key = M2RSA.RSA_pub(self._private_key.rsa)
        else:
            self._private_key = PYRSA.PrivateKey.load_pkcs1(private_key_string)
            # TODO(jamalex): load public key here automatically as well?
    
    def _remove_pem_headers(self, pem_string):
        if not pem_string.strip().startswith("-----"):
            return pem_string
        return "\n".join([line for line in pem_string.split("\n") if line and not line.startswith("---")])

    def _add_pem_headers(self, pem_string, header_string):
        context = {
            "key": self._remove_pem_headers(pem_string),
            "header_string": header_string,
        }
        return "-----BEGIN %(header_string)s-----\n%(key)s\n-----END %(header_string)s-----" % context

    def __str__(self):
        return self.get_public_key_string()


def hashed(message):
    # try to encode the message as UTF-8, replacing any invalid characters so they don't blow up the hashing
    try:
        message = message.encode("utf-8", "replace")
    except UnicodeDecodeError:
        pass
    return hashlib.sha256(message).digest()
    

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

