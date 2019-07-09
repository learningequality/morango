import io
import json

from rest_framework.parsers import BaseParser


class GzipParser(BaseParser):
    """
    Parses Gzipped data.
    """

    media_type = "application/gzip"

    def parse(self, stream, media_type=None, parser_context=None):
        """
        Parses the incoming bytestream by decompressing the gzipped data and returns the resulting data as a dictionary.
        """
        import gzip

        with gzip.GzipFile(fileobj=io.BytesIO(stream.read())) as f:
            data = f.read()
        return json.loads(data.decode("utf-8"))
