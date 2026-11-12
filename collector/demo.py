"""Convergence demo built from the two synthetic fixtures.

Arrival order is deliberately hostile:

1. sequence 1 arrives, then sequence 4 (the delete) before sequence 2;
2. the missing 2..4 range is repaired first with a truncated response and
   then with a response that carries a coverage credential;
3. a new epoch starts, after which an old-epoch notification arrives late.

Nothing here reports a webhook status as the result.  The output is the actual
resource directory, per-source watermarks/health and the repair evidence for
the gap.
"""

import json
from pathlib import Path

from .storage import Store
from .transport import CatalogClient, replay_server, repair_path


def _snapshot(store, sources=None):
    if sources is None:
        health = store.source_health()
    else:
        health = {source: store.source_health(source) for source in sources}
    return dict(
        resources=store.resources(),
        health=health,
        gaps=store.gaps(),
    )


def run_demo(events_path='fixtures/events.json',
             snapshots_path='fixtures/snapshots.json',
             database_path=None):
    events = json.loads(Path(events_path).read_text(encoding='utf-8'))['events']
    repairs = json.loads(Path(snapshots_path).read_text(encoding='utf-8'))['repairs']

    store = Store(database_path or ':memory:')
    phases = []
    try:
        # 1. Hostile arrival: the delete at sequence 4 is received before the
        #    late update at sequence 2.  Sequence 3 is absent -> a gap exists.
        received = store.receive_events([events[0], events[1]])
        gap = store.open_gaps()[0]
        phases.append(dict(
            phase='receive-delete-before-old-update',
            received=[event for event, is_new in zip([events[0], events[1]], received) if is_new],
            watermarks={epoch: store.watermark('a', epoch) for epoch in ('e1',)},
            open_gap=gap,
            health=store.source_health('a'),
        ))

        applied = store.process_pending()
        phases.append(dict(
            phase='apply-received-notifications',
            applied=applied,
            resources=store.resources(),
            resource_r=store.resource('a', 'r'),
            note='删除修订 4 先落墓碑；此时 seq2 尚未到达。',
        ))

        # 2. Repair the open gap over a real loopback catalog.  The first
        #    response is truncated; the second is complete and carries
        #    terminal=end-1.  Only the second may close the gap.
        path = repair_path(gap['source'], gap['epoch'], gap['start_seq'], gap['end_seq'])
        routes = {path: [
            dict(body=json.dumps(repairs[0], ensure_ascii=False)),
            dict(body=json.dumps(repairs[1], ensure_ascii=False)),
        ]}
        with replay_server(routes) as (base, calls):
            catalog = CatalogClient(base)
            first = store.repair_open_gaps(catalog)
            after_first = store.gaps(source='a')
            second = store.repair_open_gaps(catalog)
        phases.append(dict(
            phase='repair-truncated-response',
            scanned_path=path,
            catalog_calls=len(calls),
            outcomes=first,
            gap=after_first[0],
            note='截断响应可应用条目，但不得清除缺口。',
        ))
        phases.append(dict(
            phase='repair-covered-response',
            scanned_path=path,
            outcomes=second,
            gap=store.gaps(source='a')[0],
            note='complete=true 且带 terminal 覆盖凭据，缺口才转为 covered。',
        ))

        # 3. A new epoch starts.  Its sequence space is independent.
        store.receive_event(events[3])      # a/e2/1
        store.process_pending()
        e2_before_old_epoch = store.watermark('a', 'e2')

        # A late old-epoch notification must not move the new epoch watermark.
        store.receive_event(events[2])      # a/e1/2, the old update
        store.process_pending()
        e2_after_old_epoch = store.watermark('a', 'e2')
        phases.append(dict(
            phase='epoch-reset-and-late-old-epoch',
            new_epoch_event=events[3],
            late_old_epoch_event=events[2],
            e2_watermark_before=e2_before_old_epoch,
            e2_watermark_after=e2_after_old_epoch,
            e1_watermark=store.watermark('a', 'e1'),
            active_epoch=store.source_health('a')['active_epoch'],
            resources=store.resources(),
            note='代际水位相互独立；旧代际事件只更新 e1 行。',
        ))

        final = _snapshot(store)
        gap = next(item for item in final['gaps'] if item['source'] == 'a' and item['epoch'] == 'e1')
        r = store.resource('a', 'r')
        s = store.resource('a', 's')
        e1 = store.watermark('a', 'e1')
        e2 = store.watermark('a', 'e2')
        assertions = dict(
            delete_remains_a_tombstone=(r is not None and r['operation'] == 'delete' and r['revision'] == 4),
            old_update_cannot_republish=r is not None and r['operation'] == 'delete',
            new_epoch_resource_is_published=(s is not None and s['operation'] == 'upsert'),
            gap_covered_by_terminal=(gap['status'] == 'covered' and gap['coverage_token'] == 'end-1'),
            truncated_attempt_kept_gap_open=any(
                attempt['outcome'] == 'truncated' for attempt in gap['attempts']
            ),
            contiguous_gap_was_watermark_plus_one=(gap['range'][0] == 2 and gap['range'][1] == 4),
            e1_no_open_gap=not any(item['epoch'] == 'e1' and item['status'] == 'open' for item in final['gaps']),
            e2_watermark_isolated=(e2['contiguous_sequence'] == 1 and e2['highest_observed'] == 1),
            e1_watermark_independent=(e1['contiguous_sequence'] == 4),
            no_pending_events=(sum(item['pending_events'] for item in final['health']['a']['epochs']) == 0),
        )
        result = dict(
            fixtures=dict(events=str(events_path), snapshots=str(snapshots_path)),
            phases=phases,
            final=final,
            assertions=assertions,
            converged=all(assertions.values()),
        )
        return result
    finally:
        store.close()
