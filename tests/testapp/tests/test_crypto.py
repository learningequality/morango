from django.test import TestCase
import unittest

from morango import crypto

@unittest.skipIf(not crypto.M2CRYPTO_EXISTS, "Skipping M2Crypto tests as it does not appear to be installed.")
class TestM2Crypto(unittest.TestCase):

    def setUp(self):
        self.key = crypto.Key()
        self.pykey = crypto.Key(
            private_key_string=self.key.get_private_key_string(),
            public_key_string=self.key.get_public_key_string(),
            use_m2crypto=False)
        self.message_actual = "Hello world! Please leave a message after the tone."
        self.message_fake = "Hello world! Please leave a message after the tone..."

    def test_m2crypto_was_used(self):
        # make sure the key was generated using M2Crypto
        self.assertTrue(self.key._using_m2crypto)
        self.assertIsInstance(self.key._private_key, crypto.M2RSA.RSA)

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
        pubkey = crypto.Key(public_key_string=self.key.get_public_key_string(), use_m2crypto=True)
        sig = self.key.sign(self.message_actual)
        self.assertTrue(pubkey.verify(self.message_actual, sig))
        self.assertFalse(pubkey.verify(self.message_fake, sig))


class TestExistingKeysAndSignatures(unittest.TestCase):

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

    signature = """\xa1\xc0\xa0\xf0"}XI\xfc\xa3\x1a\xee\xc1\x057\xd9\x0end>\xbc\xc6\x12\xac\xac\xc4\xf0\xfa\x1b\x11\xce}1uL=Z\xc8y\x9b\xfc\xf5\xb5\xb0_\xdd\xec`\xe7\x06\x10\xac\x1d\xa5\ty\xc6H\xaf\xfa\xef\xd42\xd8\xec\xff>\x15o\x07\x83M\xa1\xe6 \xc4\xf3\xe0\xe1\xba)\x13N\x93\x16%\xf2F\xf3n\xa9\xcc\\9Kv\x87\x81\xa3(\xfc\xde\x0eRrA\xf3\x1d\x93p\xcew\xd6\xfb\xc4\x1b\xae\xe9\xb6W\x99AJ\xf5\xfaB\x19\x07Dk\xd5)T\x01\xd9\xbb\xda\xc5gU\xb7\t\xa3\x85\xe1\x82\xde\xe7\x13*^\xedZ\x01\x8c\x96\xbcCdD3\xb5\xc5\x93\x15\x94\xd3\x9d,#\xdb#\xfc\x11\x9c\x91a.\xafn\xf1\xb0\xb5Oah_\xaf.\xfe!m\x1c\x84r:#\x00\xe4~y*\x98\x95\x9f\xe6\xaa~3\xc0\xfb\xc9{\xbc\x86\xec\x1bm2\x8bJ\xe6\x9d\xb2\x0c\xc4\x0c\r(\xa8E\xce\xd6\xbdf]\xfd\xdc;\xec\xd3\xffj\xbf\x85\xb7\x04[\xc0\xfde\xfa\xbd\x1b\xf3r"""
    signature_base64 = "ocCg8CJ9WEn8oxruwQU32Q5uZD68xhKsrMTw+hsRzn0xdUw9Wsh5m/z1tbBf3exg5wYQrB2lCXnGSK/679Qy2Oz/PhVvB4NNoeYgxPPg4bopE06TFiXyRvNuqcxcOUt2h4GjKPzeDlJyQfMdk3DOd9b7xBuu6bZXmUFK9fpCGQdEa9UpVAHZu9rFZ1W3CaOF4YLe5xMqXu1aAYyWvENkRDO1xZMVlNOdLCPbI/wRnJFhLq9u8bC1T2FoX68u/iFtHIRyOiMA5H55KpiVn+aqfjPA+8l7vIbsG20yi0rmnbIMxAwNKKhFzta9Zl393Dvs0/9qv4W3BFvA/WX6vRvzcg=="

    @unittest.skipIf(not crypto.M2CRYPTO_EXISTS, "Skipping M2Crypto test as it does not appear to be installed.")
    def test_priv_key_with_pem_header_verification_m2crypto(self):
        key = crypto.Key(private_key_string=self.priv_key_with_pem_header, use_m2crypto=True)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    @unittest.skipIf(not crypto.M2CRYPTO_EXISTS, "Skipping M2Crypto test as it does not appear to be installed.")
    def test_priv_key_with_pem_header_unicode_verification_m2crypto(self):
        key = crypto.Key(private_key_string=self.priv_key_with_pem_header_unicode, use_m2crypto=True)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    @unittest.skipIf(not crypto.M2CRYPTO_EXISTS, "Skipping M2Crypto test as it does not appear to be installed.")
    def test_priv_key_without_pem_header_verification_m2crypto(self):
        key = crypto.Key(private_key_string=self.priv_key_without_pem_header, use_m2crypto=True)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    @unittest.skipIf(not crypto.M2CRYPTO_EXISTS, "Skipping M2Crypto test as it does not appear to be installed.")
    def test_pub_key_with_both_headers_verification_m2crypto(self):
        key = crypto.Key(public_key_string=self.pub_key_with_both_headers, use_m2crypto=True)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    @unittest.skipIf(not crypto.M2CRYPTO_EXISTS, "Skipping M2Crypto test as it does not appear to be installed.")
    def test_pub_key_with_both_headers_unicode_verification_m2crypto(self):
        key = crypto.Key(public_key_string=self.pub_key_with_both_headers_unicode, use_m2crypto=True)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    @unittest.skipIf(not crypto.M2CRYPTO_EXISTS, "Skipping M2Crypto test as it does not appear to be installed.")
    def test_pub_key_with_pkcs8_header_verification_m2crypto(self):
        key = crypto.Key(public_key_string=self.pub_key_with_pkcs8_header, use_m2crypto=True)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    @unittest.skipIf(not crypto.M2CRYPTO_EXISTS, "Skipping M2Crypto test as it does not appear to be installed.")
    def test_pub_key_with_pem_header_verification_m2crypto(self):
        key = crypto.Key(public_key_string=self.pub_key_with_pem_header, use_m2crypto=True)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    @unittest.skipIf(not crypto.M2CRYPTO_EXISTS, "Skipping M2Crypto test as it does not appear to be installed.")
    def test_pub_key_with_no_headers_verification_m2crypto(self):
        key = crypto.Key(public_key_string=self.pub_key_with_no_headers, use_m2crypto=True)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_priv_key_with_pem_header_verification_pyrsa(self):
        key = crypto.Key(private_key_string=self.priv_key_with_pem_header, use_m2crypto=False)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_priv_key_with_pem_header_verification_unicode_pyrsa(self):
        key = crypto.Key(private_key_string=self.priv_key_with_pem_header_unicode, use_m2crypto=False)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_priv_key_without_pem_header_verification_pyrsa(self):
        key = crypto.Key(private_key_string=self.priv_key_without_pem_header, use_m2crypto=False)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_pub_key_with_both_headers_verification_pyrsa(self):
        key = crypto.Key(public_key_string=self.pub_key_with_both_headers, use_m2crypto=False)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_pub_key_with_both_headers_unicode_verification_pyrsa(self):
        key = crypto.Key(public_key_string=self.pub_key_with_both_headers_unicode, use_m2crypto=False)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_pub_key_with_pkcs8_header_verification_pyrsa(self):
        key = crypto.Key(public_key_string=self.pub_key_with_pkcs8_header, use_m2crypto=False)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_pub_key_with_pem_header_verification_pyrsa(self):
        key = crypto.Key(public_key_string=self.pub_key_with_pem_header, use_m2crypto=False)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_pub_key_with_no_headers_verification_pyrsa(self):
        key = crypto.Key(public_key_string=self.pub_key_with_no_headers, use_m2crypto=False)
        self.assertTrue(key.verify(self.message_actual, self.signature))
        self.assertFalse(key.verify(self.message_fake, self.signature))

    def test_base64_signature_verification(self):
        key = crypto.Key(public_key_string=self.pub_key_with_no_headers)
        self.assertTrue(key.verify(self.message_actual, self.signature_base64))
        self.assertFalse(key.verify(self.message_fake, self.signature_base64))
