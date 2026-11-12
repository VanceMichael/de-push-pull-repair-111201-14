"""SQLite-backed notification inbox, sequence accounting and repair ledger.

The store deliberately keeps four different facts apart:

* ``events`` -- a notification was *received* (and its source/epoch/sequence
  identity was checked for duplicates and conflicts).
* ``resources`` -- a revision was *applied* to the current resource view.
  A delete leaves a tombstone row instead of removing the resource, so an old
  envelope or a late catalog item cannot make it reappear.
* ``sequence_watermarks`` -- one contiguous watermark per (source, epoch).
  The global maximum sequence is never used as a watermark.
* ``gaps`` / ``repair_attempts`` / ``coverages`` -- a missing sequence range
  and the evidence that did or did not prove it was scanned.

All mutations use ``BEGIN IMMEDIATE``.  Receivers, processors and repairers can
therefore be separate processes (or threads with separate connections): a
crash rolls the whole application of a notification back to ``pending``, and
another process can pick it up after restart.
"""

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from .domain import epoch_sort_key, normalize_event, normalize_item


def _now():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')


def _encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)


def _decode(text, default=None):
    if text is None:
        return default
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return default


def _evidence(row):
    value = _decode(row['evidence'], {})
    return value if isinstance(value, dict) else {}


class Store:
    def __init__(self, path):
        self.connection = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS raw_objects(
                digest TEXT PRIMARY KEY, payload BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS imports(
                id INTEGER PRIMARY KEY, source TEXT NOT NULL,
                digest TEXT NOT NULL REFERENCES raw_objects(digest),
                UNIQUE(source,digest));
            CREATE TABLE IF NOT EXISTS records(
                record_key TEXT PRIMARY KEY, payload TEXT NOT NULL,
                import_id INTEGER NOT NULL REFERENCES imports(id));

            CREATE TABLE IF NOT EXISTS events(
                source TEXT NOT NULL,
                epoch TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                resource TEXT NOT NULL,
                revision INTEGER NOT NULL,
                operation TEXT NOT NULL,
                value TEXT,
                payload TEXT NOT NULL,
                digest TEXT NOT NULL,
                received_at TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending',
                applied_at TEXT,
                PRIMARY KEY(source,epoch,sequence));
            CREATE INDEX IF NOT EXISTS events_pending
                ON events(state,received_at);

            CREATE TABLE IF NOT EXISTS epochs(
                source TEXT NOT NULL,
                epoch TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                event_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(source,epoch));

            CREATE TABLE IF NOT EXISTS sequence_watermarks(
                source TEXT NOT NULL,
                epoch TEXT NOT NULL,
                contiguous_sequence INTEGER NOT NULL DEFAULT 0,
                highest_observed INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(source,epoch));

            CREATE TABLE IF NOT EXISTS resources(
                source TEXT NOT NULL,
                resource TEXT NOT NULL,
                revision INTEGER NOT NULL,
                operation TEXT NOT NULL,
                value TEXT,
                epoch TEXT NOT NULL,
                sequence INTEGER,
                origin TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(source,resource));

            CREATE TABLE IF NOT EXISTS coverages(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                epoch TEXT NOT NULL,
                start_seq INTEGER NOT NULL,
                end_seq INTEGER NOT NULL,
                terminal TEXT NOT NULL,
                attempt_id INTEGER,
                created_at TEXT NOT NULL,
                UNIQUE(source,epoch,start_seq,end_seq,terminal));

            CREATE TABLE IF NOT EXISTS gaps(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                epoch TEXT NOT NULL,
                start_seq INTEGER NOT NULL,
                end_seq INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                coverage_token TEXT,
                covered_attempt_id INTEGER,
                evidence TEXT);
            CREATE INDEX IF NOT EXISTS gaps_open
                ON gaps(source,epoch,status);

            CREATE TABLE IF NOT EXISTS repair_attempts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gap_id INTEGER NOT NULL REFERENCES gaps(id),
                source TEXT NOT NULL,
                epoch TEXT NOT NULL,
                start_seq INTEGER NOT NULL,
                end_seq INTEGER NOT NULL,
                url TEXT,
                http_status INTEGER,
                complete INTEGER NOT NULL DEFAULT 0,
                terminal TEXT,
                outcome TEXT NOT NULL,
                items_json TEXT,
                error TEXT,
                created_at TEXT NOT NULL);
        """)

    @contextmanager
    def transaction(self):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield self.connection
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    # ------------------------------------------------------------------
    # Legacy import path (kept unchanged for the original sample importer)
    # ------------------------------------------------------------------
    def ingest(self, source, raw, records):
        digest = hashlib.sha256(raw).hexdigest()
        with self.transaction():
            self.connection.execute("INSERT OR IGNORE INTO raw_objects VALUES (?,?)", (digest, raw))
            self.connection.execute("INSERT OR IGNORE INTO imports(source,digest) VALUES (?,?)", (source, digest))
            import_id = self.connection.execute("SELECT id FROM imports WHERE source=? AND digest=?", (source, digest)).fetchone()[0]
            for record in records:
                key = record.get('key')
                if not isinstance(key, str) or not key:
                    raise ValueError('记录缺少有效标识')
                encoded = json.dumps(record, ensure_ascii=False, sort_keys=True, allow_nan=False)
                self.connection.execute("INSERT INTO records VALUES (?,?,?) ON CONFLICT(record_key) DO UPDATE SET payload=excluded.payload, import_id=excluded.import_id", (key, encoded, import_id))
        return digest

    def records(self):
        return [json.loads(row[0]) for row in self.connection.execute("SELECT payload FROM records ORDER BY record_key")]

    def provenance(self, key):
        row = self.connection.execute("SELECT imports.source, imports.digest FROM records JOIN imports ON imports.id=records.import_id WHERE record_key=?", (key,)).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # Receipt: receiving is not applying
    # ------------------------------------------------------------------
    def receive_event(self, event):
        return self.receive_events([event])[0]

    def receive_events(self, events):
        """Persist notifications and update sequence/gap accounting.

        Returns one boolean per input event: ``True`` for a new notification,
        ``False`` for an identical redelivery.  A different body under the same
        ``(source, epoch, sequence)`` is a protocol conflict and is rejected.
        """

        results = []
        for event in events:
            normalized = normalize_event(event)
            payload = _encode(normalized)
            digest = hashlib.sha256(payload.encode('utf-8')).hexdigest()
            received_at = _now()
            with self.transaction() as connection:
                existing = connection.execute(
                    "SELECT payload FROM events WHERE source=? AND epoch=? AND sequence=?",
                    (normalized['source'], normalized['epoch'], normalized['sequence']),
                ).fetchone()
                if existing is not None:
                    if existing['payload'] != payload:
                        raise ValueError('同一来源序列出现冲突内容')
                    results.append(False)
                    continue
                connection.execute(
                    "INSERT INTO events(source,epoch,sequence,resource,revision,operation,value,payload,digest,received_at,state) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,'pending')",
                    (
                        normalized['source'], normalized['epoch'], normalized['sequence'],
                        normalized['resource'], normalized['revision'], normalized['operation'],
                        _encode(normalized['value']), payload, digest, received_at,
                    ),
                )
                epoch_row = connection.execute(
                    "SELECT event_count FROM epochs WHERE source=? AND epoch=?",
                    (normalized['source'], normalized['epoch']),
                ).fetchone()
                if epoch_row is None:
                    connection.execute(
                        "INSERT INTO epochs(source,epoch,first_seen_at,last_seen_at,event_count) VALUES (?,?,?,?,1)",
                        (normalized['source'], normalized['epoch'], received_at, received_at),
                    )
                else:
                    connection.execute(
                        "UPDATE epochs SET last_seen_at=?, event_count=event_count+1 WHERE source=? AND epoch=?",
                        (received_at, normalized['source'], normalized['epoch']),
                    )
                self._recompute_watermark(connection, normalized['source'], normalized['epoch'])
                self._reconcile_gaps(connection, normalized['source'], normalized['epoch'])
                results.append(True)
        return results

    def pending_events(self, limit=None):
        rows = self.connection.execute(
            "SELECT * FROM events WHERE state='pending' ORDER BY received_at,source,epoch,sequence"
            + ("" if limit is None else " LIMIT ?"),
            () if limit is None else (int(limit),),
        )
        return [self._event_row(row, include_state=True) for row in rows]

    accept_event = receive_event
    accept_events = receive_events

    def events(self, state=None):
        if state is None:
            rows = self.connection.execute("SELECT * FROM events ORDER BY source,epoch,sequence")
        else:
            rows = self.connection.execute("SELECT * FROM events WHERE state=? ORDER BY source,epoch,sequence", (state,))
        return [self._event_row(row, include_state=True) for row in rows]

    def process_pending(self, limit=None):
        """Apply pending notifications, one commit per notification.

        Revision (not sequence) decides whether content wins.  Consequently any
        ordering -- or two concurrent processors -- converges to the same
        resource view.  A process crash before the commit leaves the event
        pending for the next run.
        """

        processed = 0
        while limit is None or processed < limit:
            with self.transaction() as connection:
                row = connection.execute(
                    "SELECT * FROM events WHERE state='pending' ORDER BY received_at,source,epoch,sequence LIMIT 1"
                ).fetchone()
                if row is None:
                    break
                self._apply_resource(
                    connection,
                    source=row['source'],
                    resource=row['resource'],
                    revision=row['revision'],
                    operation=row['operation'],
                    value=_decode(row['value']),
                    epoch=row['epoch'],
                    sequence=row['sequence'],
                    origin='event',
                )
                connection.execute(
                    "UPDATE events SET state='applied', applied_at=? WHERE source=? AND epoch=? AND sequence=? AND state='pending'",
                    (_now(), row['source'], row['epoch'], row['sequence']),
                )
            processed += 1
        return processed

    # Alias used by callers that think in "apply" terms.
    apply_pending = process_pending
    apply_events = process_pending
    process = process_pending

    def _event_row(self, row, include_state=False):
        event = _decode(row['payload'], {})
        if not include_state:
            return event
        event = dict(event)
        event['state'] = row['state']
        event['received_at'] = row['received_at']
        event['applied_at'] = row['applied_at']
        return event

    # ------------------------------------------------------------------
    # Revision application and tombstones
    # ------------------------------------------------------------------
    def _apply_resource(self, connection, source, resource, revision, operation, value,
                        epoch, sequence, origin):
        current = connection.execute(
            "SELECT revision,operation FROM resources WHERE source=? AND resource=?",
            (source, resource),
        ).fetchone()
        result = 'applied'
        previous = None
        if current is None:
            connection.execute(
                "INSERT INTO resources(source,resource,revision,operation,value,epoch,sequence,origin,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (source, resource, revision, operation,
                 None if operation == 'delete' else _encode(value),
                 epoch, sequence, origin, _now()),
            )
        else:
            previous = current['revision']
            if revision > current['revision']:
                connection.execute(
                    "UPDATE resources SET revision=?,operation=?,value=?,epoch=?,sequence=?,origin=?,updated_at=? "
                    "WHERE source=? AND resource=?",
                    (revision, operation, None if operation == 'delete' else _encode(value),
                     epoch, sequence, origin, _now(), source, resource),
                )
            elif revision == current['revision']:
                # A delete at the same revision wins over an upsert.  This is
                # the final safety rail if two feeds disagree about the
                # operation but not the revision: withdrawal is never undone.
                if operation == 'delete' and current['operation'] != 'delete':
                    connection.execute(
                        "UPDATE resources SET operation='delete',value=NULL,epoch=?,sequence=?,origin=?,updated_at=? "
                        "WHERE source=? AND resource=?",
                        (epoch, sequence, origin, _now(), source, resource),
                    )
                else:
                    result = 'equal'
            else:
                result = 'stale'
        current_after = connection.execute(
            "SELECT revision,operation FROM resources WHERE source=? AND resource=?",
            (source, resource),
        ).fetchone()
        return dict(
            resource=resource,
            revision=revision,
            operation=operation,
            result=result,
            previous_revision=previous,
            current_revision=current_after['revision'] if current_after else revision,
            current_operation=current_after['operation'] if current_after else operation,
        )

    def resources(self, source=None):
        if source is None:
            rows = self.connection.execute(
                "SELECT * FROM resources ORDER BY source,resource"
            )
        else:
            rows = self.connection.execute(
                "SELECT * FROM resources WHERE source=? ORDER BY resource", (source,)
            )
        result = []
        for row in rows:
            result.append(dict(
                key=f"{row['source']}:{row['resource']}",
                source=row['source'],
                resource=row['resource'],
                revision=row['revision'],
                operation=row['operation'],
                deleted=row['operation'] == 'delete',
                value=_decode(row['value']),
                epoch=row['epoch'],
                sequence=row['sequence'],
                origin=row['origin'],
                updated_at=row['updated_at'],
            ))
        return result

    def published_resources(self, source=None):
        return [row for row in self.resources(source) if row['operation'] == 'upsert']

    def resource(self, source, resource):
        for row in self.resources(source):
            if row['resource'] == resource:
                return row
        return None

    def tombstones(self, source=None):
        return [row for row in self.resources(source) if row['operation'] == 'delete']

    # ------------------------------------------------------------------
    # Sequence watermarks and gaps
    # ------------------------------------------------------------------
    def _recompute_watermark(self, connection, source, epoch):
        observed = [
            row['sequence'] for row in connection.execute(
                "SELECT sequence FROM events WHERE source=? AND epoch=?",
                (source, epoch),
            )
        ]
        coverages = [
            (row['start_seq'], row['end_seq']) for row in connection.execute(
                "SELECT start_seq,end_seq FROM coverages WHERE source=? AND epoch=?",
                (source, epoch),
            )
        ]
        intervals = [(sequence, sequence) for sequence in observed] + coverages
        intervals.sort()
        contiguous = 0
        for start, end in intervals:
            if start <= contiguous + 1 and end > contiguous:
                contiguous = end
        highest = max(observed, default=0)
        connection.execute(
            "INSERT INTO sequence_watermarks(source,epoch,contiguous_sequence,highest_observed,updated_at) "
            "VALUES (?,?,?,?,?) "
            "ON CONFLICT(source,epoch) DO UPDATE SET "
            "contiguous_sequence=excluded.contiguous_sequence,"
            "highest_observed=excluded.highest_observed,"
            "updated_at=excluded.updated_at",
            (source, epoch, contiguous, highest, _now()),
        )
        return contiguous, highest

    def _reconcile_gaps(self, connection, source, epoch):
        contiguous, highest = self._recompute_watermark(connection, source, epoch)
        open_rows = connection.execute(
            "SELECT * FROM gaps WHERE source=? AND epoch=? AND status='open' ORDER BY id",
            (source, epoch),
        ).fetchall()
        if contiguous < highest:
            start_seq, end_seq = contiguous + 1, highest
            if open_rows:
                row = open_rows[0]
                evidence = _evidence(row)
                evidence['range'] = [start_seq, end_seq]
                evidence['updated_at'] = _now()
                connection.execute(
                    "UPDATE gaps SET start_seq=?,end_seq=?,updated_at=?,evidence=? WHERE id=?",
                    (start_seq, end_seq, _now(), _encode(evidence), row['id']),
                )
                for extra in open_rows[1:]:
                    evidence = _evidence(extra)
                    evidence.update(method='notifications', resolved_at=_now())
                    connection.execute(
                        "UPDATE gaps SET status='filled',updated_at=?,evidence=? WHERE id=?",
                        (_now(), _encode(evidence), extra['id']),
                    )
            else:
                now = _now()
                connection.execute(
                    "INSERT INTO gaps(source,epoch,start_seq,end_seq,status,created_at,updated_at,evidence) "
                    "VALUES (?,?,?,?,'open',?,?,?)",
                    (source, epoch, start_seq, end_seq, now, now,
                     _encode(dict(method='sequence_gap', range=[start_seq, end_seq], detected_at=now))),
                )
        else:
            # Notifications (or prior coverage) filled the whole interval.
            for row in open_rows:
                evidence = _evidence(row)
                evidence.update(method='notifications', resolved_at=_now())
                connection.execute(
                    "UPDATE gaps SET status='filled',updated_at=?,evidence=? WHERE id=?",
                    (_now(), _encode(evidence), row['id']),
                )
        return contiguous, highest

    def watermark(self, source, epoch):
        row = self.connection.execute(
            "SELECT * FROM sequence_watermarks WHERE source=? AND epoch=?",
            (source, epoch),
        ).fetchone()
        if row is None:
            return dict(source=source, epoch=epoch, contiguous_sequence=0,
                        watermark=0, highest_observed=0, updated_at=None)
        return dict(
            source=row['source'],
            epoch=row['epoch'],
            contiguous_sequence=row['contiguous_sequence'],
            watermark=row['contiguous_sequence'],
            highest_observed=row['highest_observed'],
            updated_at=row['updated_at'],
        )

    sequence_watermark = watermark

    def watermarks(self, source=None):
        rows = self.connection.execute(
            "SELECT * FROM sequence_watermarks"
            + ("" if source is None else " WHERE source=?")
            + " ORDER BY source,epoch",
            () if source is None else (source,),
        )
        return [dict(
            source=row['source'],
            epoch=row['epoch'],
            contiguous_sequence=row['contiguous_sequence'],
            watermark=row['contiguous_sequence'],
            highest_observed=row['highest_observed'],
            updated_at=row['updated_at'],
        ) for row in rows]

    def coverages(self, source=None, epoch=None):
        clauses = []
        arguments = []
        if source is not None:
            clauses.append('source=?')
            arguments.append(source)
        if epoch is not None:
            clauses.append('epoch=?')
            arguments.append(epoch)
        query = "SELECT * FROM coverages"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY id"
        return [dict(
            id=row['id'],
            source=row['source'],
            epoch=row['epoch'],
            range=[row['start_seq'], row['end_seq']],
            terminal=row['terminal'],
            attempt_id=row['attempt_id'],
            created_at=row['created_at'],
        ) for row in self.connection.execute(query, tuple(arguments))]

    def gaps(self, status=None, source=None, epoch=None):
        clauses = []
        arguments = []
        if status is not None:
            clauses.append("status=?")
            arguments.append(status)
        if source is not None:
            clauses.append("source=?")
            arguments.append(source)
        if epoch is not None:
            clauses.append("epoch=?")
            arguments.append(epoch)
        query = "SELECT * FROM gaps"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY id"
        return [self._gap_detail(row) for row in self.connection.execute(query, tuple(arguments))]

    gap_details = gaps

    def open_gaps(self, source=None):
        return self.gaps(status='open', source=source)

    def _gap_detail(self, row):
        attempts = []
        for attempt in self.connection.execute(
            "SELECT * FROM repair_attempts WHERE gap_id=? ORDER BY id", (row['id'],)
        ):
            attempts.append(dict(
                id=attempt['id'],
                gap_id=attempt['gap_id'],
                source=attempt['source'],
                epoch=attempt['epoch'],
                range=[attempt['start_seq'], attempt['end_seq']],
                url=attempt['url'],
                http_status=attempt['http_status'],
                complete=bool(attempt['complete']),
                terminal=attempt['terminal'],
                outcome=attempt['outcome'],
                items=_decode(attempt['items_json'], []),
                error=attempt['error'],
                created_at=attempt['created_at'],
            ))
        missing = self._missing_sequences(row['source'], row['epoch'],
                                          row['start_seq'], row['end_seq'])
        return dict(
            id=row['id'],
            source=row['source'],
            epoch=row['epoch'],
            range=[row['start_seq'], row['end_seq']],
            start_seq=row['start_seq'],
            end_seq=row['end_seq'],
            status=row['status'],
            missing_sequences=missing,
            coverage_token=row['coverage_token'],
            covered_attempt_id=row['covered_attempt_id'],
            evidence=_evidence(row),
            attempts=attempts,
            created_at=row['created_at'],
            updated_at=row['updated_at'],
        )

    def _missing_sequences(self, source, epoch, start_seq, end_seq):
        observed = {
            row['sequence'] for row in self.connection.execute(
                "SELECT sequence FROM events WHERE source=? AND epoch=? "
                "AND sequence>=? AND sequence<=?",
                (source, epoch, start_seq, end_seq),
            )
        }
        coverages = [
            (row['start_seq'], row['end_seq']) for row in self.connection.execute(
                "SELECT start_seq,end_seq FROM coverages WHERE source=? AND epoch=?",
                (source, epoch),
            )
        ]
        missing = []
        for sequence in range(start_seq, end_seq + 1):
            if sequence in observed:
                continue
            if any(start <= sequence <= end for start, end in coverages):
                continue
            missing.append(sequence)
        return missing

    # ------------------------------------------------------------------
    # Repair protocol
    # ------------------------------------------------------------------
    def repair_gap(self, gap_id, payload, http_status=200, url=None):
        """Apply one catalog response to one gap.

        A gap is cleared only when the response contains both ``complete=true``
        and a non-empty coverage credential (``terminal``) whose range covers
        the requested interval.  Truncated, failed, malformed and
        credential-less responses may apply valid items under the revision
        guard, but they leave the gap open.
        """

        gap = self.connection.execute("SELECT * FROM gaps WHERE id=?", (gap_id,)).fetchone()
        if gap is None:
            raise ValueError('缺口不存在')
        start_seq, end_seq = gap['start_seq'], gap['end_seq']

        if not (200 <= int(http_status or 0) < 300):
            return self.record_repair_failure(
                gap_id, f'目录响应状态 {http_status}', http_status=http_status, url=url)

        if not isinstance(payload, dict):
            return self.record_repair_failure(
                gap_id, '目录响应不是对象', http_status=http_status, url=url)

        complete_value = payload.get('complete')
        if isinstance(complete_value, str):
            complete = complete_value.strip().lower() in ('true', 'yes', '1')
        else:
            complete = complete_value is True or complete_value == 1
        terminal_raw = payload.get('terminal')
        terminal = None if terminal_raw is None else str(terminal_raw)
        if terminal == '':
            terminal = None

        returned_range = payload.get('range')
        returned = None
        if isinstance(returned_range, (list, tuple)) and len(returned_range) == 2:
            try:
                returned = (int(returned_range[0]), int(returned_range[1]))
            except (TypeError, ValueError):
                returned = None

        effective_range = returned if returned is not None else (start_seq, end_seq)
        range_covers = effective_range[0] <= start_seq and effective_range[1] >= end_seq
        coverage_valid = complete and terminal is not None and range_covers

        if not complete:
            outcome = 'truncated'
        elif terminal is None:
            outcome = 'invalid_no_terminal'
        elif not range_covers:
            outcome = 'invalid_range'
        else:
            outcome = 'covered'

        items_raw = payload.get('items', [])
        if not isinstance(items_raw, list):
            return self.record_repair_failure(
                gap_id, '目录条目不是数组', http_status=http_status, url=url)
        try:
            items = [normalize_item(item) for item in items_raw]
            # Force JSON serialization now so a bad value cannot abort a
            # transaction after part of the batch was applied.
            for item in items:
                if item['operation'] == 'upsert':
                    _encode(item['value'])
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            return self.record_repair_failure(
                gap_id, f'目录条目无效: {error}', http_status=http_status, url=url)

        now = _now()
        with self.transaction() as connection:
            gap = connection.execute("SELECT * FROM gaps WHERE id=?", (gap_id,)).fetchone()
            if gap is None:
                raise ValueError('缺口不存在')
            start_seq, end_seq = gap['start_seq'], gap['end_seq']
            attempt_id = self._insert_attempt(
                connection, gap, url, http_status, complete, terminal,
                outcome, items_raw, None, now,
            )
            item_results = []
            for item in items:
                item_results.append(self._apply_resource(
                    connection,
                    source=gap['source'],
                    resource=item['resource'],
                    revision=item['revision'],
                    operation=item['operation'],
                    value=item['value'],
                    epoch=gap['epoch'],
                    sequence=None,
                    origin='repair',
                ))
            connection.execute(
                "UPDATE repair_attempts SET items_json=? WHERE id=?",
                (_encode(dict(range=effective_range, items=item_results)), attempt_id),
            )

            if coverage_valid:
                connection.execute(
                    "INSERT OR IGNORE INTO coverages(source,epoch,start_seq,end_seq,terminal,attempt_id,created_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (gap['source'], gap['epoch'], effective_range[0], effective_range[1],
                     terminal, attempt_id, now),
                )
                coverage_id = connection.execute(
                    "SELECT id FROM coverages WHERE source=? AND epoch=? AND start_seq=? AND end_seq=? AND terminal=?",
                    (gap['source'], gap['epoch'], effective_range[0], effective_range[1], terminal),
                ).fetchone()['id']
                evidence = _evidence(gap)
                evidence.update(
                    method='catalog_coverage',
                    range=[start_seq, end_seq],
                    coverage_range=[effective_range[0], effective_range[1]],
                    coverage_token=terminal,
                    coverage_id=coverage_id,
                    attempt_id=attempt_id,
                    complete=True,
                    items=item_results,
                    covered_at=now,
                    updated_at=now,
                )
                for stale_key in ('last_attempt', 'last_outcome', 'last_error',
                                  'last_http_status', 'last_attempt_at'):
                    evidence.pop(stale_key, None)
                connection.execute(
                    "UPDATE gaps SET status='covered',coverage_token=?,covered_attempt_id=?,updated_at=?,evidence=? WHERE id=?",
                    (terminal, attempt_id, now, _encode(evidence), gap['id']),
                )
                # Closing the old gap may expose a *new* gap beyond the
                # covered range (for example, a notification arrived during
                # the repair).  Reconciliation creates that new record.
                self._reconcile_gaps(connection, gap['source'], gap['epoch'])
            else:
                evidence = _evidence(gap)
                evidence.update(
                    last_attempt=attempt_id,
                    last_outcome=outcome,
                    last_attempt_at=now,
                    last_http_status=http_status,
                    range=[start_seq, end_seq],
                )
                if terminal is None:
                    evidence['last_error'] = '覆盖凭据缺失，缺口保持打开'
                elif not range_covers:
                    evidence['last_error'] = '目录覆盖范围未覆盖请求范围，缺口保持打开'
                else:
                    evidence['last_error'] = '补拉截断，缺口保持打开'
                connection.execute(
                    "UPDATE gaps SET updated_at=?,evidence=? WHERE id=?",
                    (now, _encode(evidence), gap['id']),
                )
                self._reconcile_gaps(connection, gap['source'], gap['epoch'])
        current_gap = self.connection.execute(
            "SELECT * FROM gaps WHERE id=?", (gap_id,)
        ).fetchone()
        missing = (self._missing_sequences(current_gap['source'], current_gap['epoch'],
                                           current_gap['start_seq'], current_gap['end_seq'])
                   if current_gap is not None else [])
        return dict(
            gap_id=gap_id,
            outcome=outcome,
            covered=coverage_valid,
            range=[start_seq, end_seq],
            coverage_token=terminal,
            attempt_id=attempt_id,
            items=item_results,
            missing=missing,
        )

    def _insert_attempt(self, connection, gap, url, http_status, complete, terminal,
                        outcome, items, error, created_at):
        cursor = connection.execute(
            "INSERT INTO repair_attempts(gap_id,source,epoch,start_seq,end_seq,url,http_status,complete,terminal,outcome,items_json,error,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (gap['id'], gap['source'], gap['epoch'], gap['start_seq'], gap['end_seq'],
             url, http_status, 1 if complete else 0, terminal, outcome,
             _encode(items), error, created_at),
        )
        return cursor.lastrowid

    def record_repair_failure(self, gap_id, error, http_status=None, url=None):
        now = _now()
        with self.transaction() as connection:
            gap = connection.execute("SELECT * FROM gaps WHERE id=?", (gap_id,)).fetchone()
            if gap is None:
                raise ValueError('缺口不存在')
            attempt_id = self._insert_attempt(
                connection, gap, url, http_status, False, None,
                'failed', [], str(error), now,
            )
            evidence = _evidence(gap)
            evidence.update(
                last_attempt=attempt_id,
                last_outcome='failed',
                last_error=str(error),
                last_http_status=http_status,
                last_attempt_at=now,
                range=[gap['start_seq'], gap['end_seq']],
            )
            connection.execute(
                "UPDATE gaps SET updated_at=?,evidence=? WHERE id=?",
                (now, _encode(evidence), gap['id']),
            )
            # A failed pull never changes coverage or the contiguous watermark.
            self._reconcile_gaps(connection, gap['source'], gap['epoch'])
        return dict(gap_id=gap_id, outcome='failed', covered=False,
                    range=[gap['start_seq'], gap['end_seq']], attempt_id=attempt_id,
                    items=[], error=str(error))

    def repair_open_gaps(self, catalog):
        """Repair every open gap against a catalog query client."""

        results = []
        for gap in self.open_gaps():
            try:
                fetched = catalog.fetch(gap['source'], gap['epoch'],
                                        gap['start_seq'], gap['end_seq'])
                if isinstance(fetched, tuple):
                    status, url, payload = fetched
                else:
                    status, url, payload = 200, None, fetched
                if not (200 <= int(status or 0) < 300):
                    results.append(self.record_repair_failure(
                        gap['id'], f'目录响应状态 {status}', http_status=status, url=url))
                else:
                    results.append(self.repair_gap(gap['id'], payload,
                                                   http_status=status, url=url))
            except Exception as error:  # network/JSON failure must not close a gap
                results.append(self.record_repair_failure(
                    gap['id'], f'补拉失败: {error}'))
        return results

    # ------------------------------------------------------------------
    # Health and evidence
    # ------------------------------------------------------------------
    def source_health(self, source=None):
        if source is not None:
            return self._source_health(source)
        sources = sorted({
            row['source'] for row in self.connection.execute("SELECT DISTINCT source FROM epochs")
        } | {
            row['source'] for row in self.connection.execute("SELECT DISTINCT source FROM resources")
        } | {
            row['source'] for row in self.connection.execute("SELECT DISTINCT source FROM events")
        })
        return {name: self._source_health(name) for name in sources}

    health = source_health
    status = source_health
    repair = repair_gap
    repair_gaps = repair_open_gaps
    gap_evidence = gaps

    def _source_health(self, source):
        epoch_rows = self.connection.execute(
            "SELECT * FROM epochs WHERE source=? ORDER BY epoch", (source,)
        ).fetchall()
        epochs = [row['epoch'] for row in epoch_rows]
        active_epoch = max(epochs, key=epoch_sort_key) if epochs else None

        pending_by_epoch = {
            row['epoch']: row['count'] for row in self.connection.execute(
                "SELECT epoch,count(*) AS count FROM events WHERE source=? AND state='pending' GROUP BY epoch",
                (source,),
            )
        }
        open_by_epoch = {
            row['epoch']: row['count'] for row in self.connection.execute(
                "SELECT epoch,count(*) AS count FROM gaps WHERE source=? AND status='open' GROUP BY epoch",
                (source,),
            )
        }
        coverage_by_epoch = {
            row['epoch']: row['count'] for row in self.connection.execute(
                "SELECT epoch,count(*) AS count FROM coverages WHERE source=? GROUP BY epoch",
                (source,),
            )
        }

        epoch_details = []
        for epoch in sorted(epochs, key=epoch_sort_key):
            watermark = self.watermark(source, epoch)
            epoch_gaps = open_by_epoch.get(epoch, 0)
            epoch_pending = pending_by_epoch.get(epoch, 0)
            epoch_status = ('degraded' if epoch_gaps
                            else 'backlog' if epoch_pending else 'healthy')
            epoch_details.append(dict(
                epoch=epoch,
                active=(epoch == active_epoch),
                status=epoch_status,
                contiguous_sequence=watermark['contiguous_sequence'],
                watermark=watermark['watermark'],
                highest_observed=watermark['highest_observed'],
                pending_events=epoch_pending,
                open_gaps=epoch_gaps,
                coverages=coverage_by_epoch.get(epoch, 0),
            ))

        pending = sum(pending_by_epoch.values())
        open_gaps = sum(open_by_epoch.values())
        published = self.connection.execute(
            "SELECT count(*) AS count FROM resources WHERE source=? AND operation='upsert'", (source,)
        ).fetchone()['count']
        tombstones = self.connection.execute(
            "SELECT count(*) AS count FROM resources WHERE source=? AND operation='delete'", (source,)
        ).fetchone()['count']
        last_event = self.connection.execute(
            "SELECT max(received_at) AS value FROM events WHERE source=?", (source,)
        ).fetchone()['value']
        last_repair = self.connection.execute(
            "SELECT max(created_at) AS value FROM repair_attempts WHERE source=?", (source,)
        ).fetchone()['value']

        if open_gaps:
            status = 'degraded'
        elif pending:
            status = 'backlog'
        elif epoch_rows or published or tombstones:
            status = 'healthy'
        else:
            status = 'unknown'

        return dict(
            source=source,
            status=status,
            active_epoch=active_epoch,
            epochs=epoch_details,
            pending_events=pending,
            open_gaps=open_gaps,
            published=published,
            tombstones=tombstones,
            last_event_at=last_event,
            last_repair_at=last_repair,
        )

    def close(self):
        self.connection.close()
