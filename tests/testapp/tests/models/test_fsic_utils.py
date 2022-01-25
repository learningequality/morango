from morango.models.fsic_utils import expand_fsic_for_use
from morango.models.fsic_utils import remove_redundant_instance_counters
from morango.models.fsic_utils import calculate_directional_fsic_diff_v2

from django.test import TestCase

class TestFSICUtils(TestCase):
    
    def test_expand_fsic_for_use(self):
        source_fsic = {
            "super": {
                "p": {
                    "a": 5,
                    "b": 3,
                    "c": 7,
                },
            },
            "sub": {
                "p1": {
                    "a": 1,
                    "b": 9,
                    "d": 2,
                },
                "p1i": {
                    "e": 5,
                },
                "p2i": {
                    "e": 5,
                },
            },
        }
        expected_fsic = {
            "p1": {
                "a": 5,  # from super, because it was larger
                "b": 9,  # from sub, because it was larger
                "c": 7,  # from super, because it didn't exist in sub
                "d": 2,  # from sub, because it didn't exist in super
            },
            "p1i": {  # only instance here, because others from super were covered by p1
                "e": 5, 
            },
            "p2i": {  # but no prefix in sub for this one, so it does inherit from super
                "a": 5,
                "b": 3,
                "c": 7,
                "e": 5,
            },
        }
        self.assertEqual(expand_fsic_for_use(source_fsic), expected_fsic)

    def test_remove_redundant_instance_counters(self):
        source_fsic = {
            "super": {
                "p": {
                    "a": 5,
                    "b": 3,
                    "c": 7,
                },
                "p3": {
                    "a": 1,  # will be stripped out, because lower than p's counter
                    "c": 8,  # will be kept, because higher than p's counter
                    "d": 14,  # will be kept, as it's not in p's counter
                },
            },
            "sub": {
                "p1": {
                    "a": 5,  # will be stripped out, because lower than p's counter
                    "b": 9,  # will be kept, because higher than p's counter
                    "c": 7,  # will be stripped out, because same as p's counter
                    "d": 2,  # will be kept, because it's not in p's counter
                },
                "p1i": {
                    "e": 5,  # will be kept, because it's not in p's counter
                    "c": 4,  # will be stripped out, because lower than p's and p1's counter
                    "d": 1,  # will be stripped out, because lower than p1's counter
                },
                "p1j": {  # will be an empty dict, because these counters are all <= p's counters
                    "b": 3,
                    "c": 5,
                },
                "p2i": {
                    "a": 5,  # will be stripped out, because same as p's counter
                    "e": 5,  # will be kept, because it's not in p's counter
                },
                "p3i": {
                    "a": 8,  # will be kept, because it's higher than p's and p3's counter
                    "c": 5,  # will be stripped out, because lower than p's and p3's counter
                    "d": 2,  # will be stripped out, because lower than p3's counter
                },
            }
        }
        expected_fsic = {
            "super": {
                "p": {
                    "a": 5,
                    "b": 3,
                    "c": 7,
                },
                "p3": {
                    "c": 8,
                    "d": 14,
                },
            },
            "sub": {
                "p1": {
                    "b": 9,
                    "d": 2,
                },
                "p1i": {
                    "e": 5, 
                },
                "p1j": {},
                "p2i": {
                    "e": 5,
                },
                "p3i": {
                    "a": 8,
                },
            }
        }
        remove_redundant_instance_counters(source_fsic)
        self.assertEqual(source_fsic, expected_fsic)

    def test_calculate_directional_fsic_diff_v2(self):
        sending_fsic = {
            "p": {
                "a": 5,
                "c": 7,
            },
            "p1": {
                "b": 9,
                "d": 2,
            },
            "p1i": {
                "a": 7,
                "e": 6,
                "f": 1,
            },
            "p2": {
                "a": 8,
                "q": 5,
            },
        }
        receiving_fsic = {
            "p": {
                "a": 3,  # will be included, because it's lower than sender
                "b": 4,  # won't be included, because it doesn't exist in sender
                "c": 9,  # won't be included, because it's higher than sender
            },
            "p1": {  # will be excluded, because it's the same as in sender
                "b": 9,
                "d": 2,
            },
            "p1i": {
                "a": 6,  # will be included, because it's lower than sender
                "e": 5,  # will be included, because it's lower than sender
                "c": 9,  # won't be included, because it's higher than in p in sender
                # "f"  # will be included at 0, as it doesn't exist in receiver
            },
            # "p2": {
            #     "a"  # will be included at 3, because it's in prefix partition p in receiver
            #     "q"  # will be included at 0, because it's not in the receiver
            # },
            "p3": {  # will be excluded, because it's not in the source
                "a": 2,
                "c": 3,
            }
        }
        expected_diff = {
            "p": {
                "a": 3,
            },
            "p1i": {
                "a": 6,
                "e": 5,
                "f": 0,
            },
            "p2": {
                "a": 3,
                "q": 0,
            },
        }
        self.assertEqual(calculate_directional_fsic_diff_v2(sending_fsic, receiving_fsic), expected_diff)

    def test_calculate_directional_fsic_diff_v2_identical(self):
        sending_fsic = receiving_fsic = {
            "p": {
                "a": 5,
                "c": 7,
            },
            "p1": {
                "b": 9,
                "d": 2,
            },
            "p1i": {
                "a": 7,
                "e": 6,
                "f": 1,
            },
            "p2": {
                "a": 8,
                "q": 5,
            },
        }
        expected_diff = {}
        self.assertEqual(calculate_directional_fsic_diff_v2(sending_fsic, receiving_fsic), expected_diff)

    def test_calculate_directional_fsic_diff_v2_receiver_is_higher(self):
        sending_fsic = {
            "p": {
                "a": 5,
                "c": 7,
            },
            "p1": {
                "b": 9,
                "d": 2,
            },
            "p1i": {
                "a": 7,
                "e": 6,
                "f": 1,
            },
            "p2": {
                "a": 8,
                "q": 5,
            },
        }
        receiving_fsic = {
            "p": {
                "a": 6,
                "c": 9,
            },
            "p1": {
                "b": 11,
                "d": 12,
            },
            "p1i": {
                "a": 71,
                "e": 16,
                "f": 21,
            },
            "p2": {
                "a": 48,
                "q": 51,
            },
        }
        expected_diff = {}
        self.assertEqual(calculate_directional_fsic_diff_v2(sending_fsic, receiving_fsic), expected_diff)