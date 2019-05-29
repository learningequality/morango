from rest_framework.parsers import BaseParser
import gzip
import json

class GzipParser(BaseParser):
    """
    Parses Gzipped data.
    """
    media_type = 'application/gzip'

    def parse(self, stream, media_type=None, parser_context=None):
        """
        Parses the incoming bytestream by decompressing the gzipped data and returns the resulting data as a dictionary.
        """
        return json.loads(gzip.decompress(stream.read()).decode('utf-8'))
