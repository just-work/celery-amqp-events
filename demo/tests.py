from unittest import TestCase


class SmokeTestCase(TestCase):
    """
    There is nothing to test yet, just smoke imports checks.
    """

    # noinspection PyMethodMayBeStatic
    def test_ok(self):
        __import__('demo.tasks')
        __import__('demo.events')
