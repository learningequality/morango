"""
humanhash: Human-readable representations of digests.
The simplest ways to use this module are the :func:`humanize` and :func:`uuid`
functions. For tighter control over the output, see :class:`HumanHasher`.
"""
import uuid
from django.utils import six

# Copyright (c) 2014 SUNET. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are
# permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice, this list of
#       conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright notice, this list
#       of conditions and the following disclaimer in the documentation and/or other materials
#       provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY SUNET ``AS IS'' AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL SUNET OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are those of the
# authors and should not be interpreted as representing official policies, either expressed
# or implied, of SUNET.
#
"""
Python implementation of proquints.
Proquints are PRO-nouncable QUINT-uplets of alternating unambiguous consonants
and vowels. See http://arxiv.org/html/0901.4016 for more info.
Copyright (c) 2014 SUNET. All rights reserved.
See the source file for complete license statement.
"""

__version__ = "0.1.0"
__copyright__ = "SUNET"
__organization__ = "SUNET"
__license__ = "BSD"
__authors__ = ["Fredrik Thulin"]

__all__ = []

CONSONANTS = "bdfghjklmnprstvz"
VOWELS = "aiou"


def from_int(data):
    """
    :params data: integer
    :returns: proquint made from input data
    :type data: int
    :rtype: string
    """
    if not isinstance(data, six.integer_types):
        raise TypeError("Input must be integer")

    res = []
    while data > 0 or not res:
        for j in range(5):
            if not j % 2:
                res += CONSONANTS[(data & 0xF)]
                data >>= 4
            else:
                res += VOWELS[(data & 0x3)]
                data >>= 2
        if data > 0:
            res += "-"
    res.reverse()
    return "".join(res)


def to_int(data):
    """
    :params data: proquint
    :returns: proquint decoded into an integer
    :type data: string
    :rtype: int
    """
    if not isinstance(data, six.string_types):
        raise TypeError("Input must be string")

    res = 0
    for part in data.split("-"):
        if len(part) != 5:
            raise ValueError("Malformed proquint")
        for j in range(5):
            try:
                if not j % 2:
                    res <<= 4
                    res |= CONSONANTS.index(part[j])
                else:
                    res <<= 2
                    res |= VOWELS.index(part[j])
            except ValueError:
                raise ValueError("Unknown character '{!s}' in proquint".format(part[j]))
    return res


def generate():
    """
    :returns: proquint
    :rtype: int
    """
    return from_int(int(uuid.uuid4().hex[:8], 16)).replace("-", "")
