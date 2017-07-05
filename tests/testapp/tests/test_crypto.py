# coding=utf-8
import unittest

from morango import crypto


class CrossKeySigVerificationMixin(object):

    def setupKeys(self, base_key):
        self.key = base_key
        self.pykey = crypto.PythonRSAKey(
            private_key_string=self.key.get_private_key_string(),
        )
        if crypto.M2CRYPTO_EXISTS:
            self.m2cryptokey = crypto.M2CryptoKey(
                private_key_string=self.key.get_private_key_string(),
                public_key_string=self.key.get_public_key_string()
            )
        if crypto.CRYPTOGRAPHY_EXISTS:
            self.cryptokey = crypto.CryptographyKey(
                private_key_string=self.key.get_private_key_string(),
                public_key_string=self.key.get_public_key_string()
            )
        self.message_actual = "Hello world! Please leave a message after the tone."
        self.message_fake = "Hello world! Please leave a message after the tone..."


@unittest.skipIf(not crypto.M2CRYPTO_EXISTS, "Skipping M2Crypto tests as it does not appear to be installed.")
class TestM2CryptoWithPyRSA(CrossKeySigVerificationMixin, unittest.TestCase):

    def setUp(self):
        self.setupKeys(crypto.M2CryptoKey())

    def test_pyrsa_sig_verification_with_m2crypto(self):
        # make sure something signed with a pyrsa key can be verified by m2crypto
        sig = self.pykey.sign(self.message_actual)
        self.assertTrue(self.key.verify(self.message_actual, sig))
        # make sure it doesn't verify for a different message
        self.assertFalse(self.key.verify(self.message_fake, sig))

    def test_m2crypto_sig_verification_with_pyrsa(self):
        # make sure something signed with an m2crypto key can be verified by pyrsa
        sig = self.key.sign(self.message_actual)
        self.assertTrue(self.pykey.verify(self.message_actual, sig))
        # make sure it doesn't verify for a different message
        self.assertFalse(self.pykey.verify(self.message_fake, sig))

    def test_pubkey_verification_m2crypto(self):
        pubkey = crypto.M2CryptoKey(public_key_string=self.key.get_public_key_string())
        sig = self.key.sign(self.message_actual)
        self.assertTrue(pubkey.verify(self.message_actual, sig))
        self.assertFalse(pubkey.verify(self.message_fake, sig))

    def test_pubkey_verification_pyrsa(self):
        pubkey = crypto.PythonRSAKey(public_key_string=self.pykey.get_public_key_string())
        sig = self.pykey.sign(self.message_actual)
        self.assertTrue(pubkey.verify(self.message_actual, sig))
        self.assertFalse(pubkey.verify(self.message_fake, sig))


@unittest.skipIf(not crypto.CRYPTOGRAPHY_EXISTS, "Skipping python-cryptography test as it does not appear to be installed.")
class TestCryptoWithPyRSA(CrossKeySigVerificationMixin, unittest.TestCase):

    def setUp(self):
        self.setupKeys(crypto.CryptographyKey())

    def test_pyrsa_sig_verification_with_crypto(self):
        # make sure something signed with a pyrsa key can be verified by m2crypto
        sig = self.pykey.sign(self.message_actual)
        self.assertTrue(self.key.verify(self.message_actual, sig))
        # make sure it doesn't verify for a different message
        self.assertFalse(self.key.verify(self.message_fake, sig))

    def test_crypto_sig_verification_with_pyrsa(self):
        # make sure something signed with an m2crypto key can be verified by pyrsa
        sig = self.key.sign(self.message_actual)
        self.assertTrue(self.pykey.verify(self.message_actual, sig))
        # make sure it doesn't verify for a different message
        self.assertFalse(self.pykey.verify(self.message_fake, sig))

    def test_pubkey_verification_crypto(self):
        pubkey = crypto.CryptographyKey(public_key_string=self.key.get_public_key_string())
        sig = self.key.sign(self.message_actual)
        self.assertTrue(pubkey.verify(self.message_actual, sig))
        self.assertFalse(pubkey.verify(self.message_fake, sig))


@unittest.skipIf(not crypto.CRYPTOGRAPHY_EXISTS, "Skipping python-cryptography test as it does not appear to be installed.")
@unittest.skipIf(not crypto.M2CRYPTO_EXISTS, "Skipping M2Crypto tests as it does not appear to be installed.")
class TestCryptoWithM2Crypto(CrossKeySigVerificationMixin, unittest.TestCase):

    def setUp(self):
        self.setupKeys(crypto.CryptographyKey())

    def test_m2crypto_sig_verification_with_crypto(self):
        # make sure something signed with a pyrsa key can be verified by m2crypto
        sig = self.m2cryptokey.sign(self.message_actual)
        self.assertTrue(self.key.verify(self.message_actual, sig))
        # make sure it doesn't verify for a different message
        self.assertFalse(self.key.verify(self.message_fake, sig))

    def test_crypto_sig_verification_with_m2crypto(self):
        # make sure something signed with an m2crypto key can be verified by pyrsa
        sig = self.key.sign(self.message_actual)
        self.assertTrue(self.m2cryptokey.verify(self.message_actual, sig))
        # make sure it doesn't verify for a different message
        self.assertFalse(self.m2cryptokey.verify(self.message_fake, sig))


class ExistingKeyParsingMixin(object):

    priv_key_with_pem_header = "-----BEGIN RSA PRIVATE KEY-----\nMIIEogIBAAKCAQEAuABOgZEZ0pxp2hoYnTrYFoqQtzOEeTrjwTULV2v+zjyuT4f/\nIZylz4TH1MgmUMbn7/nu6dsfCYc87hx16fbhcUdZpiAW0Lb0mfnHxUwJKrBHmdr/\nMF8smN1a4OjOJ5O9ugAoijhG+Pb+SUh4tFSRv1vx68CyfxMSK2g/5jGJWlyh1K9Y\noBKXOtsXQQppl+4N4Stve9qFfsyjIW/FNlya3qmQDe9p9r5Ir7YEIS090rCOCEA3\nyiQ8gFThzCVK8Xlu3R/vclrhfvxhJWSJS5z5QRQ0QaZY+/A4b940yDLluRGViHKq\nagMvaKrcTO/fOAa257eSTFUyn7GjxAa9vy8i2QIDAQABAoIBAAiuWwXZ5rH9FzFn\nEco5QICvwOwjzhg6Iwy2h/Zz7e2lB0RRUkQvs9L6nML5PnWJLOAxlogKAojcjI9f\nYGDNeQ1zJFOmJ+1o9FlfY4F3eOc+seIcZvXNR7lemC8MTM2pNsZTw5Xh2dddL9od\nRTSc2NOCbwOEb+d26uCJZpphs+1DZQ5UYPgdu3N+wna5+OdZra1waqZqm1DFKbFT\nVx1PEPH4zyX5jhNP7MFW4W/u96gqAHVaPbkiycuEnxClidZapFrAqaQjFWCA6OPz\nCoNsy+n5fGD5eDIwI/AQQULRAQm35IW6zFmHrdB2q5Aeg87cclhdRnLXx7HQzfWZ\nKetBogECgYEA48mcaU69f4o8E7OCqGKP905MOXT9b1oRC7HRIhcSV3QKlxl/0VY/\nL9A64hn92ByWsyfWbkWjchK3mz2KGi9j1TaoErMLcfxWhcmvHCHIkYjmpilspKfp\nHNAM530C24+7POMcQiT6Q+KIapWyLffpmXHQPd4Z9p11KFx6gQ6hHtECgYEAzspg\nKD0889I32wa0fohHe6fk1Wtv/Fz+SRJ5LYAk/CCfisYDTA2ejayChSXzfPc0oPlb\n9EqBNd6tShhTc1VrJp3F/M4nPN/ZvHzVA/ndu5vpeAiBzdtzjttp3W7Ea01bym++\nOYvnhoDLrG80GCH0nJDCqtuqoYxLvB3Ek8EmlYkCgYAxgSh4Dn/Kjx1dXr7/n2QQ\naDjSp+VIZPedZgjAcukujm6axhTsRuU2m/egGev8IsJxry/ACWxrJzw2BdrUtAXr\nWZSPc9AB9shLDTj8US9Iycruw8PzyPY1p9WWHaoYU5VqtyT2DxlA1aO2HlB6Aw4G\npiCOwY089p12pxqMn8ROcQKBgHJZVnLp6hqp1Fk5i/WsRlsKrG+XyYUzpymhHYEb\nq1gAcji65nfX0CVnj4UxR0ODL4cUXNTpnim7yPeAHCVaxrXD6Qeyt9/hqPWh0ekw\n8nwb6y6FBcJf57bHffMEnXj4fhmjUP1hb9Xgwr/HfncZz7oEEqGIdwJ+IiMUEu/h\njwSBAoGACN/OWrCnLDDqb9kXXIsqx+oJpo311PW39JipU1yEB5Z1PAHw6/qm0PzU\nwCQ+UUbIhdrfdWEs+pPVa4qFNIjVatNdOL5heJzY6ZGQOCV2xv+qX9vuuN962rUk\nmQW3SLGIqqvUDV3Z2nfBwV5L3qbPuGm21PliMUQQOggjx+UIjOo=\n-----END RSA PRIVATE KEY-----"
    priv_key_without_pem_header = "MIIEogIBAAKCAQEAuABOgZEZ0pxp2hoYnTrYFoqQtzOEeTrjwTULV2v+zjyuT4f/\nIZylz4TH1MgmUMbn7/nu6dsfCYc87hx16fbhcUdZpiAW0Lb0mfnHxUwJKrBHmdr/\nMF8smN1a4OjOJ5O9ugAoijhG+Pb+SUh4tFSRv1vx68CyfxMSK2g/5jGJWlyh1K9Y\noBKXOtsXQQppl+4N4Stve9qFfsyjIW/FNlya3qmQDe9p9r5Ir7YEIS090rCOCEA3\nyiQ8gFThzCVK8Xlu3R/vclrhfvxhJWSJS5z5QRQ0QaZY+/A4b940yDLluRGViHKq\nagMvaKrcTO/fOAa257eSTFUyn7GjxAa9vy8i2QIDAQABAoIBAAiuWwXZ5rH9FzFn\nEco5QICvwOwjzhg6Iwy2h/Zz7e2lB0RRUkQvs9L6nML5PnWJLOAxlogKAojcjI9f\nYGDNeQ1zJFOmJ+1o9FlfY4F3eOc+seIcZvXNR7lemC8MTM2pNsZTw5Xh2dddL9od\nRTSc2NOCbwOEb+d26uCJZpphs+1DZQ5UYPgdu3N+wna5+OdZra1waqZqm1DFKbFT\nVx1PEPH4zyX5jhNP7MFW4W/u96gqAHVaPbkiycuEnxClidZapFrAqaQjFWCA6OPz\nCoNsy+n5fGD5eDIwI/AQQULRAQm35IW6zFmHrdB2q5Aeg87cclhdRnLXx7HQzfWZ\nKetBogECgYEA48mcaU69f4o8E7OCqGKP905MOXT9b1oRC7HRIhcSV3QKlxl/0VY/\nL9A64hn92ByWsyfWbkWjchK3mz2KGi9j1TaoErMLcfxWhcmvHCHIkYjmpilspKfp\nHNAM530C24+7POMcQiT6Q+KIapWyLffpmXHQPd4Z9p11KFx6gQ6hHtECgYEAzspg\nKD0889I32wa0fohHe6fk1Wtv/Fz+SRJ5LYAk/CCfisYDTA2ejayChSXzfPc0oPlb\n9EqBNd6tShhTc1VrJp3F/M4nPN/ZvHzVA/ndu5vpeAiBzdtzjttp3W7Ea01bym++\nOYvnhoDLrG80GCH0nJDCqtuqoYxLvB3Ek8EmlYkCgYAxgSh4Dn/Kjx1dXr7/n2QQ\naDjSp+VIZPedZgjAcukujm6axhTsRuU2m/egGev8IsJxry/ACWxrJzw2BdrUtAXr\nWZSPc9AB9shLDTj8US9Iycruw8PzyPY1p9WWHaoYU5VqtyT2DxlA1aO2HlB6Aw4G\npiCOwY089p12pxqMn8ROcQKBgHJZVnLp6hqp1Fk5i/WsRlsKrG+XyYUzpymhHYEb\nq1gAcji65nfX0CVnj4UxR0ODL4cUXNTpnim7yPeAHCVaxrXD6Qeyt9/hqPWh0ekw\n8nwb6y6FBcJf57bHffMEnXj4fhmjUP1hb9Xgwr/HfncZz7oEEqGIdwJ+IiMUEu/h\njwSBAoGACN/OWrCnLDDqb9kXXIsqx+oJpo311PW39JipU1yEB5Z1PAHw6/qm0PzU\nwCQ+UUbIhdrfdWEs+pPVa4qFNIjVatNdOL5heJzY6ZGQOCV2xv+qX9vuuN962rUk\nmQW3SLGIqqvUDV3Z2nfBwV5L3qbPuGm21PliMUQQOggjx+UIjOo="
    priv_key_with_pem_header_unicode = u"-----BEGIN RSA PRIVATE KEY-----\nMIIEogIBAAKCAQEAuABOgZEZ0pxp2hoYnTrYFoqQtzOEeTrjwTULV2v+zjyuT4f/\nIZylz4TH1MgmUMbn7/nu6dsfCYc87hx16fbhcUdZpiAW0Lb0mfnHxUwJKrBHmdr/\nMF8smN1a4OjOJ5O9ugAoijhG+Pb+SUh4tFSRv1vx68CyfxMSK2g/5jGJWlyh1K9Y\noBKXOtsXQQppl+4N4Stve9qFfsyjIW/FNlya3qmQDe9p9r5Ir7YEIS090rCOCEA3\nyiQ8gFThzCVK8Xlu3R/vclrhfvxhJWSJS5z5QRQ0QaZY+/A4b940yDLluRGViHKq\nagMvaKrcTO/fOAa257eSTFUyn7GjxAa9vy8i2QIDAQABAoIBAAiuWwXZ5rH9FzFn\nEco5QICvwOwjzhg6Iwy2h/Zz7e2lB0RRUkQvs9L6nML5PnWJLOAxlogKAojcjI9f\nYGDNeQ1zJFOmJ+1o9FlfY4F3eOc+seIcZvXNR7lemC8MTM2pNsZTw5Xh2dddL9od\nRTSc2NOCbwOEb+d26uCJZpphs+1DZQ5UYPgdu3N+wna5+OdZra1waqZqm1DFKbFT\nVx1PEPH4zyX5jhNP7MFW4W/u96gqAHVaPbkiycuEnxClidZapFrAqaQjFWCA6OPz\nCoNsy+n5fGD5eDIwI/AQQULRAQm35IW6zFmHrdB2q5Aeg87cclhdRnLXx7HQzfWZ\nKetBogECgYEA48mcaU69f4o8E7OCqGKP905MOXT9b1oRC7HRIhcSV3QKlxl/0VY/\nL9A64hn92ByWsyfWbkWjchK3mz2KGi9j1TaoErMLcfxWhcmvHCHIkYjmpilspKfp\nHNAM530C24+7POMcQiT6Q+KIapWyLffpmXHQPd4Z9p11KFx6gQ6hHtECgYEAzspg\nKD0889I32wa0fohHe6fk1Wtv/Fz+SRJ5LYAk/CCfisYDTA2ejayChSXzfPc0oPlb\n9EqBNd6tShhTc1VrJp3F/M4nPN/ZvHzVA/ndu5vpeAiBzdtzjttp3W7Ea01bym++\nOYvnhoDLrG80GCH0nJDCqtuqoYxLvB3Ek8EmlYkCgYAxgSh4Dn/Kjx1dXr7/n2QQ\naDjSp+VIZPedZgjAcukujm6axhTsRuU2m/egGev8IsJxry/ACWxrJzw2BdrUtAXr\nWZSPc9AB9shLDTj8US9Iycruw8PzyPY1p9WWHaoYU5VqtyT2DxlA1aO2HlB6Aw4G\npiCOwY089p12pxqMn8ROcQKBgHJZVnLp6hqp1Fk5i/WsRlsKrG+XyYUzpymhHYEb\nq1gAcji65nfX0CVnj4UxR0ODL4cUXNTpnim7yPeAHCVaxrXD6Qeyt9/hqPWh0ekw\n8nwb6y6FBcJf57bHffMEnXj4fhmjUP1hb9Xgwr/HfncZz7oEEqGIdwJ+IiMUEu/h\njwSBAoGACN/OWrCnLDDqb9kXXIsqx+oJpo311PW39JipU1yEB5Z1PAHw6/qm0PzU\nwCQ+UUbIhdrfdWEs+pPVa4qFNIjVatNdOL5heJzY6ZGQOCV2xv+qX9vuuN962rUk\nmQW3SLGIqqvUDV3Z2nfBwV5L3qbPuGm21PliMUQQOggjx+UIjOo=\n-----END RSA PRIVATE KEY-----"

    pub_key_with_both_headers = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuABOgZEZ0pxp2hoYnTrY\nFoqQtzOEeTrjwTULV2v+zjyuT4f/IZylz4TH1MgmUMbn7/nu6dsfCYc87hx16fbh\ncUdZpiAW0Lb0mfnHxUwJKrBHmdr/MF8smN1a4OjOJ5O9ugAoijhG+Pb+SUh4tFSR\nv1vx68CyfxMSK2g/5jGJWlyh1K9YoBKXOtsXQQppl+4N4Stve9qFfsyjIW/FNlya\n3qmQDe9p9r5Ir7YEIS090rCOCEA3yiQ8gFThzCVK8Xlu3R/vclrhfvxhJWSJS5z5\nQRQ0QaZY+/A4b940yDLluRGViHKqagMvaKrcTO/fOAa257eSTFUyn7GjxAa9vy8i\n2QIDAQAB\n-----END PUBLIC KEY-----\n"
    pub_key_with_pkcs8_header = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuABOgZEZ0pxp2hoYnTrY\nFoqQtzOEeTrjwTULV2v+zjyuT4f/IZylz4TH1MgmUMbn7/nu6dsfCYc87hx16fbh\ncUdZpiAW0Lb0mfnHxUwJKrBHmdr/MF8smN1a4OjOJ5O9ugAoijhG+Pb+SUh4tFSR\nv1vx68CyfxMSK2g/5jGJWlyh1K9YoBKXOtsXQQppl+4N4Stve9qFfsyjIW/FNlya\n3qmQDe9p9r5Ir7YEIS090rCOCEA3yiQ8gFThzCVK8Xlu3R/vclrhfvxhJWSJS5z5\nQRQ0QaZY+/A4b940yDLluRGViHKqagMvaKrcTO/fOAa257eSTFUyn7GjxAa9vy8i\n2QIDAQAB"
    pub_key_with_pem_header = "-----BEGIN RSA PUBLIC KEY-----\nMIIBCgKCAQEAuABOgZEZ0pxp2hoYnTrY\nFoqQtzOEeTrjwTULV2v+zjyuT4f/IZylz4TH1MgmUMbn7/nu6dsfCYc87hx16fbh\ncUdZpiAW0Lb0mfnHxUwJKrBHmdr/MF8smN1a4OjOJ5O9ugAoijhG+Pb+SUh4tFSR\nv1vx68CyfxMSK2g/5jGJWlyh1K9YoBKXOtsXQQppl+4N4Stve9qFfsyjIW/FNlya\n3qmQDe9p9r5Ir7YEIS090rCOCEA3yiQ8gFThzCVK8Xlu3R/vclrhfvxhJWSJS5z5\nQRQ0QaZY+/A4b940yDLluRGViHKqagMvaKrcTO/fOAa257eSTFUyn7GjxAa9vy8i\n2QIDAQAB\n-----END RSA PUBLIC KEY-----\n"
    pub_key_with_no_headers = "MIIBCgKCAQEAuABOgZEZ0pxp2hoYnTrY\nFoqQtzOEeTrjwTULV2v+zjyuT4f/IZylz4TH1MgmUMbn7/nu6dsfCYc87hx16fbh\ncUdZpiAW0Lb0mfnHxUwJKrBHmdr/MF8smN1a4OjOJ5O9ugAoijhG+Pb+SUh4tFSR\nv1vx68CyfxMSK2g/5jGJWlyh1K9YoBKXOtsXQQppl+4N4Stve9qFfsyjIW/FNlya\n3qmQDe9p9r5Ir7YEIS090rCOCEA3yiQ8gFThzCVK8Xlu3R/vclrhfvxhJWSJS5z5\nQRQ0QaZY+/A4b940yDLluRGViHKqagMvaKrcTO/fOAa257eSTFUyn7GjxAa9vy8i\n2QIDAQAB"
    pub_key_with_both_headers_unicode = u"-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuABOgZEZ0pxp2hoYnTrY\nFoqQtzOEeTrjwTULV2v+zjyuT4f/IZylz4TH1MgmUMbn7/nu6dsfCYc87hx16fbh\ncUdZpiAW0Lb0mfnHxUwJKrBHmdr/MF8smN1a4OjOJ5O9ugAoijhG+Pb+SUh4tFSR\nv1vx68CyfxMSK2g/5jGJWlyh1K9YoBKXOtsXQQppl+4N4Stve9qFfsyjIW/FNlya\n3qmQDe9p9r5Ir7YEIS090rCOCEA3yiQ8gFThzCVK8Xlu3R/vclrhfvxhJWSJS5z5\nQRQ0QaZY+/A4b940yDLluRGViHKqagMvaKrcTO/fOAa257eSTFUyn7GjxAa9vy8i\n2QIDAQAB\n-----END PUBLIC KEY-----\n"

    message_actual = "This is the real message."
    message_fake = "This is not the real message."

    signature = "ocCg8CJ9WEn8oxruwQU32Q5uZD68xhKsrMTw+hsRzn0xdUw9Wsh5m/z1tbBf3exg5wYQrB2lCXnGSK/679Qy2Oz/PhVvB4NNoeYgxPPg4bopE06TFiXyRvNuqcxcOUt2h4GjKPzeDlJyQfMdk3DOd9b7xBuu6bZXmUFK9fpCGQdEa9UpVAHZu9rFZ1W3CaOF4YLe5xMqXu1aAYyWvENkRDO1xZMVlNOdLCPbI/wRnJFhLq9u8bC1T2FoX68u/iFtHIRyOiMA5H55KpiVn+aqfjPA+8l7vIbsG20yi0rmnbIMxAwNKKhFzta9Zl393Dvs0/9qv4W3BFvA/WX6vRvzcg=="

    def test_priv_key_with_pem_header_verification(self):
        key = self.key_class(private_key_string=self.priv_key_with_pem_header)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_priv_key_with_pem_header_verification_unicode(self):
        key = self.key_class(private_key_string=self.priv_key_with_pem_header_unicode)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_priv_key_without_pem_header_verification(self):
        key = self.key_class(private_key_string=self.priv_key_without_pem_header)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_pub_key_with_both_headers_verification(self):
        key = self.key_class(public_key_string=self.pub_key_with_both_headers)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_pub_key_with_both_headers_unicode_verification(self):
        key = self.key_class(public_key_string=self.pub_key_with_both_headers_unicode)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_pub_key_with_pkcs8_header_verification(self):
        key = self.key_class(public_key_string=self.pub_key_with_pkcs8_header)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_pub_key_with_pem_header_verification(self):
        key = self.key_class(public_key_string=self.pub_key_with_pem_header)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_pub_key_with_no_headers_verification(self):
        key = self.key_class(public_key_string=self.pub_key_with_no_headers)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))


class TestExistingKeyParsingWithPythonRSA(unittest.TestCase, ExistingKeyParsingMixin):

    def setUp(self):
        self.key_class = crypto.PythonRSAKey


@unittest.skipIf(not crypto.M2CRYPTO_EXISTS, "Skipping M2Crypto test as it does not appear to be installed.")
class TestExistingKeyParsingWithM2Crypto(unittest.TestCase, ExistingKeyParsingMixin):

    def setUp(self):
        self.key_class = crypto.M2CryptoKey


@unittest.skipIf(not crypto.CRYPTOGRAPHY_EXISTS, "Skipping python-cryptography test as it does not appear to be installed.")
class TestExistingKeyParsingWithPythonCryptography(unittest.TestCase, ExistingKeyParsingMixin):

    def setUp(self):
        self.key_class = crypto.CryptographyKey


class StringEncodingVariationsMixin(object):

    priv_key_string = "-----BEGIN RSA PRIVATE KEY-----\nMIIEogIBAAKCAQEAuABOgZEZ0pxp2hoYnTrYFoqQtzOEeTrjwTULV2v+zjyuT4f/\nIZylz4TH1MgmUMbn7/nu6dsfCYc87hx16fbhcUdZpiAW0Lb0mfnHxUwJKrBHmdr/\nMF8smN1a4OjOJ5O9ugAoijhG+Pb+SUh4tFSRv1vx68CyfxMSK2g/5jGJWlyh1K9Y\noBKXOtsXQQppl+4N4Stve9qFfsyjIW/FNlya3qmQDe9p9r5Ir7YEIS090rCOCEA3\nyiQ8gFThzCVK8Xlu3R/vclrhfvxhJWSJS5z5QRQ0QaZY+/A4b940yDLluRGViHKq\nagMvaKrcTO/fOAa257eSTFUyn7GjxAa9vy8i2QIDAQABAoIBAAiuWwXZ5rH9FzFn\nEco5QICvwOwjzhg6Iwy2h/Zz7e2lB0RRUkQvs9L6nML5PnWJLOAxlogKAojcjI9f\nYGDNeQ1zJFOmJ+1o9FlfY4F3eOc+seIcZvXNR7lemC8MTM2pNsZTw5Xh2dddL9od\nRTSc2NOCbwOEb+d26uCJZpphs+1DZQ5UYPgdu3N+wna5+OdZra1waqZqm1DFKbFT\nVx1PEPH4zyX5jhNP7MFW4W/u96gqAHVaPbkiycuEnxClidZapFrAqaQjFWCA6OPz\nCoNsy+n5fGD5eDIwI/AQQULRAQm35IW6zFmHrdB2q5Aeg87cclhdRnLXx7HQzfWZ\nKetBogECgYEA48mcaU69f4o8E7OCqGKP905MOXT9b1oRC7HRIhcSV3QKlxl/0VY/\nL9A64hn92ByWsyfWbkWjchK3mz2KGi9j1TaoErMLcfxWhcmvHCHIkYjmpilspKfp\nHNAM530C24+7POMcQiT6Q+KIapWyLffpmXHQPd4Z9p11KFx6gQ6hHtECgYEAzspg\nKD0889I32wa0fohHe6fk1Wtv/Fz+SRJ5LYAk/CCfisYDTA2ejayChSXzfPc0oPlb\n9EqBNd6tShhTc1VrJp3F/M4nPN/ZvHzVA/ndu5vpeAiBzdtzjttp3W7Ea01bym++\nOYvnhoDLrG80GCH0nJDCqtuqoYxLvB3Ek8EmlYkCgYAxgSh4Dn/Kjx1dXr7/n2QQ\naDjSp+VIZPedZgjAcukujm6axhTsRuU2m/egGev8IsJxry/ACWxrJzw2BdrUtAXr\nWZSPc9AB9shLDTj8US9Iycruw8PzyPY1p9WWHaoYU5VqtyT2DxlA1aO2HlB6Aw4G\npiCOwY089p12pxqMn8ROcQKBgHJZVnLp6hqp1Fk5i/WsRlsKrG+XyYUzpymhHYEb\nq1gAcji65nfX0CVnj4UxR0ODL4cUXNTpnim7yPeAHCVaxrXD6Qeyt9/hqPWh0ekw\n8nwb6y6FBcJf57bHffMEnXj4fhmjUP1hb9Xgwr/HfncZz7oEEqGIdwJ+IiMUEu/h\njwSBAoGACN/OWrCnLDDqb9kXXIsqx+oJpo311PW39JipU1yEB5Z1PAHw6/qm0PzU\nwCQ+UUbIhdrfdWEs+pPVa4qFNIjVatNdOL5heJzY6ZGQOCV2xv+qX9vuuN962rUk\nmQW3SLGIqqvUDV3Z2nfBwV5L3qbPuGm21PliMUQQOggjx+UIjOo=\n-----END RSA PRIVATE KEY-----"

    normal_string = "here is a snowman: ☃!"
    byte_string = b"here is a snowman: \xe2\x98\x83!"
    unicode_string = u"here is a snowman: ☃!"

    signature = "Ml95gR/0M6wpW+wNo+4DZjumlTzVQALw75gngJh3h4fRRkBQGSnfGvKq/FUTre+OMVmgwSGAE91+A8d5CDmCWVosUczQIJGUB4KBa4sVCBn3cZFD3MDL23Bc1EOebMCYt6WGznXu+Wb/m/AyBHIsNGCU3H0cTtWT9ST8NhEF3mHb/nei9mgE8n8KL1UmlIGSsbTxbdEK9JMgf5ZdYjm3p6aHoj/P1EmTFt31mScO4z8IBuH1RdfqMFdU8JKMOnQy6I4f943Y5NXH/pR0d+Y2JMEsoUvHmNU2FDp1SofiQSNBsXOBSBYa09/SgL6dFtqf7JKwepI7LSKSL1suAwF6Kg=="

    def test_signatures_match_across_encodings(self):
        self.assertEqual(self.signature, self.key.sign(self.normal_string))
        self.assertEqual(self.signature, self.key.sign(self.byte_string))
        self.assertEqual(self.signature, self.key.sign(self.unicode_string))

    def test_signature_verifies_across_encodings(self):
        self.assertTrue(self.key.verify(self.normal_string, self.signature))
        self.assertTrue(self.key.verify(self.byte_string, self.signature))
        self.assertTrue(self.key.verify(self.unicode_string, self.signature))


class TestStringEncodingVariationsWithPythonRSA(unittest.TestCase, StringEncodingVariationsMixin):

    def setUp(self):
        self.key = crypto.PythonRSAKey(private_key_string=self.priv_key_string)


@unittest.skipIf(not crypto.M2CRYPTO_EXISTS, "Skipping M2Crypto test as it does not appear to be installed.")
class TestStringEncodingVariationsWithM2Crypto(unittest.TestCase, StringEncodingVariationsMixin):

    def setUp(self):
        self.key = crypto.M2CryptoKey(private_key_string=self.priv_key_string)


@unittest.skipIf(not crypto.CRYPTOGRAPHY_EXISTS, "Skipping python-cryptography test as it does not appear to be installed.")
class TestStringEncodingVariationsWithPythonCryptography(unittest.TestCase, StringEncodingVariationsMixin):

    def setUp(self):
        self.key = crypto.CryptographyKey(private_key_string=self.priv_key_string)
