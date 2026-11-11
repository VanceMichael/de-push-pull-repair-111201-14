import json
from pathlib import Path
import unittest

from collector.domain import *


class DomainTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(Path('fixtures/sample.json').read_text())

    def test_domain_behavior(self):
        inbox = Inbox()
        event = self.payload['events'][0]
        self.assertTrue(inbox.receive(event))
        self.assertFalse(inbox.receive(event))
        with self.assertRaises(ValueError):
            inbox.receive(dict(event, revision=2))

    def test_deterministic_observations(self):
        self.assertEqual(collect(self.payload), collect(self.payload))
