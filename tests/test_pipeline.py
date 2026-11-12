import json
import tempfile
import threading
import unittest
from pathlib import Path

from collector.storage import Store
from collector.transport import CatalogClient, replay_server, repair_path


def fixture(name):
    return json.loads((Path('fixtures') / name).read_text(encoding='utf-8'))


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temporary.name) / 'store.sqlite3')

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def test_duplicate_and_conflict(self):
        event = fixture('events.json')['events'][0]
        self.assertTrue(self.store.receive_event(event))
        self.assertFalse(self.store.receive_event(dict(event)))
        with self.assertRaises(ValueError):
            self.store.receive_event(dict(event, revision=99))

    def test_receipt_is_not_application_and_survives_reopen(self):
        events = fixture('events.json')['events']
        self.store.receive_events([events[0], events[1]])
        self.assertEqual(len(self.store.pending_events()), 2)
        self.assertEqual(self.store.process_pending(limit=1), 1)
        path = Path(self.temporary.name) / 'store.sqlite3'
        self.store.close()

        second = Store(path)
        try:
            self.assertEqual(len(second.pending_events()), 1)
            self.assertEqual(second.process_pending(), 1)
            self.assertEqual(second.resource('a', 'r')['operation'], 'delete')
            self.assertEqual(second.resource('a', 'r')['revision'], 4)
            self.assertEqual(second.connection.execute(
                "SELECT count(*) FROM events WHERE state='pending'").fetchone()[0], 0)
        finally:
            second.close()
        self.store = Store(path)  # let tearDown close a valid connection


class RevisionAndTombstoneTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temporary.name) / 'store.sqlite3')

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def test_delete_first_blocks_late_low_revision_update(self):
        events = fixture('events.json')['events']
        # Receive the delete (sequence 4) before the update (sequence 2), then
        # process in exactly the same hostile order.
        self.store.receive_events([events[0], events[1], events[2]])
        self.store.process_pending()
        resource = self.store.resource('a', 'r')
        self.assertEqual(resource['operation'], 'delete')
        self.assertEqual(resource['revision'], 4)
        self.assertEqual(self.store.published_resources(), [])

    def test_same_revision_delete_wins(self):
        self.store.receive_events([
            dict(source='a', epoch='e1', sequence=1, resource='r', revision=3,
                 operation='upsert', value='old'),
            dict(source='a', epoch='e1', sequence=2, resource='r', revision=3,
                 operation='delete'),
        ])
        self.store.process_pending()
        self.assertEqual(self.store.resource('a', 'r')['operation'], 'delete')

    def test_newer_revision_resurrects_legitimately(self):
        self.store.receive_events([
            dict(source='a', epoch='e1', sequence=1, resource='r', revision=4,
                 operation='delete'),
            dict(source='a', epoch='e1', sequence=2, resource='r', revision=5,
                 operation='upsert', value='new'),
        ])
        self.store.process_pending()
        self.assertEqual(self.store.resource('a', 'r')['operation'], 'upsert')
        self.assertEqual(self.store.resource('a', 'r')['revision'], 5)


class WatermarkAndGapTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temporary.name) / 'store.sqlite3')

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def test_global_max_is_not_a_watermark(self):
        # Source x has a hole; source y's larger sequence must not hide it.
        self.store.receive_events([
            dict(source='x', epoch='e1', sequence=1, resource='rx', revision=1, operation='upsert'),
            dict(source='x', epoch='e1', sequence=3, resource='rx', revision=2, operation='upsert'),
            dict(source='y', epoch='e1', sequence=9, resource='ry', revision=1, operation='upsert'),
        ])
        x = self.store.watermark('x', 'e1')
        y = self.store.watermark('y', 'e1')
        self.assertEqual((x['contiguous_sequence'], x['highest_observed']), (1, 3))
        self.assertEqual((y['contiguous_sequence'], y['highest_observed']), (0, 9))
        self.assertEqual(self.store.gaps(source='x', status='open')[0]['range'], [2, 3])

    def test_watermarks_are_per_epoch(self):
        events = fixture('events.json')['events']
        self.store.receive_events([events[0], events[3]])
        self.assertEqual(self.store.watermark('a', 'e1')['contiguous_sequence'], 1)
        self.assertEqual(self.store.watermark('a', 'e2')['contiguous_sequence'], 1)
        self.store.receive_events([events[2]])  # late old epoch
        self.assertEqual(self.store.watermark('a', 'e2')['contiguous_sequence'], 1)
        self.assertEqual(self.store.source_health('a')['active_epoch'], 'e2')


class RepairTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temporary.name) / 'store.sqlite3')

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def _open_gap(self):
        events = fixture('events.json')['events']
        self.store.receive_events([events[0], events[1]])
        self.store.process_pending()
        gap = self.store.open_gaps()[0]
        self.assertEqual(gap['range'], [2, 4])
        return gap

    def test_truncated_repair_cannot_clear_gap(self):
        gap = self._open_gap()
        repairs = fixture('snapshots.json')['repairs']
        result = self.store.repair_gap(gap['id'], repairs[0])
        self.assertEqual(result['outcome'], 'truncated')
        self.assertFalse(result['covered'])
        self.assertEqual(self.store.gaps(source='a')[0]['status'], 'open')
        # rev3 is older than the rev4 tombstone, so the truncated item cannot
        # resurrect the withdrawn resource either.
        self.assertEqual(self.store.resource('a', 'r')['operation'], 'delete')
        self.assertEqual(result['items'][0]['result'], 'stale')

    def test_complete_with_coverage_clears_gap_and_keeps_tombstone(self):
        gap = self._open_gap()
        repairs = fixture('snapshots.json')['repairs']
        self.store.repair_gap(gap['id'], repairs[0])
        result = self.store.repair_gap(gap['id'], repairs[1])
        self.assertEqual(result['outcome'], 'covered')
        detail = self.store.gaps(source='a')[0]
        self.assertEqual(detail['status'], 'covered')
        self.assertEqual(detail['coverage_token'], 'end-1')
        self.assertEqual([attempt['outcome'] for attempt in detail['attempts']],
                         ['truncated', 'covered'])
        self.assertEqual(self.store.resource('a', 'r')['operation'], 'delete')
        self.assertEqual(self.store.source_health('a')['open_gaps'], 0)

    def test_complete_without_terminal_or_wrong_range_stays_open(self):
        gap = self._open_gap()
        base = dict(complete=True, range=[2, 4], items=[])
        result = self.store.repair_gap(gap['id'], dict(base, terminal=None))
        self.assertEqual(result['outcome'], 'invalid_no_terminal')
        result = self.store.repair_gap(gap['id'], dict(base, range=[3, 4], terminal='t'))
        self.assertEqual(result['outcome'], 'invalid_range')
        self.assertEqual(self.store.gaps(source='a')[0]['status'], 'open')

    def test_failed_http_repair_keeps_gap_open(self):
        gap = self._open_gap()
        path = repair_path(gap['source'], gap['epoch'], gap['start_seq'], gap['end_seq'])
        with replay_server({path: [dict(status=503, body='unavailable')]}) as (base, calls):
            results = self.store.repair_open_gaps(CatalogClient(base))
        self.assertEqual(len(calls), 1)
        self.assertEqual(results[0]['outcome'], 'failed')
        detail = self.store.gaps(source='a')[0]
        self.assertEqual(detail['status'], 'open')
        self.assertEqual(detail['attempts'][-1]['outcome'], 'failed')
        self.assertEqual(detail['attempts'][-1]['http_status'], 503)
        self.assertEqual(self.store.watermark('a', 'e1')['contiguous_sequence'], 1)

    def test_open_gap_repair_uses_http_catalog(self):
        gap = self._open_gap()
        repairs = fixture('snapshots.json')['repairs']
        path = repair_path(gap['source'], gap['epoch'], gap['start_seq'], gap['end_seq'])
        routes = {path: [dict(body=json.dumps(repairs[0])),
                         dict(body=json.dumps(repairs[1]))]}
        with replay_server(routes) as (base, calls):
            catalog = CatalogClient(base)
            first = self.store.repair_open_gaps(catalog)
            second = self.store.repair_open_gaps(catalog)
        self.assertEqual(len(calls), 2)
        self.assertEqual(first[0]['outcome'], 'truncated')
        self.assertEqual(second[0]['outcome'], 'covered')
        self.assertEqual(self.store.gaps(source='a')[0]['status'], 'covered')


class ConcurrencyTests(unittest.TestCase):
    def test_two_processors_converge_and_each_event_is_applied_once(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'store.sqlite3'
            seed = Store(path)
            events = fixture('events.json')['events']
            seed.receive_events(events)
            seed.close()

            results = []
            errors = []

            def worker():
                try:
                    store = Store(path)
                    try:
                        results.append(store.process_pending())
                    finally:
                        store.close()
                except BaseException as error:  # pragma: no cover - only on failure
                    errors.append(error)

            threads = [threading.Thread(target=worker) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            self.assertEqual(errors, [])
            self.assertEqual(sum(results), len(events))
            store = Store(path)
            try:
                self.assertEqual(store.connection.execute(
                    "SELECT count(*) FROM events WHERE state='pending'").fetchone()[0], 0)
                resource = store.resource('a', 'r')
                self.assertEqual((resource['operation'], resource['revision']), ('delete', 4))
                self.assertEqual(store.resource('a', 's')['operation'], 'upsert')
            finally:
                store.close()


class DemoTests(unittest.TestCase):
    def test_demo_converges_all_scenarios(self):
        from collector.demo import run_demo
        with tempfile.TemporaryDirectory() as directory:
            result = run_demo(database_path=Path(directory) / 'demo.sqlite3')
        self.assertTrue(result['converged'])
        self.assertTrue(all(result['assertions'].values()))
        gap = result['final']['gaps'][0]
        self.assertEqual(gap['status'], 'covered')
        self.assertEqual(gap['coverage_token'], 'end-1')
        self.assertEqual(gap['attempts'][0]['outcome'], 'truncated')
        self.assertEqual(result['final']['health']['a']['active_epoch'], 'e2')
        self.assertEqual(result['final']['resources'][0]['operation'], 'delete')


if __name__ == '__main__':
    unittest.main()
