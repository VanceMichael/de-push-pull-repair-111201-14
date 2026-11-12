"""Notification formats and revision/epoch ordering helpers.

The receiver used to keep notifications in memory.  The helpers below are the
small, dependency-free part of the validation that both the in-memory inbox
and the SQLite-backed receiver use, so a malformed notification is rejected
before it can become part of a watermark or a gap.
"""

from copy import deepcopy

OPERATIONS = ('upsert', 'delete')


class Inbox:
    """In-memory compatibility inbox (kept for the original import path)."""

    def __init__(self):
        self.events = {}

    def receive(self, event):
        key = event['source'], event['epoch'], int(event['sequence'])
        if key in self.events:
            if self.events[key] != event:
                raise ValueError('同一来源序列出现冲突内容')
            return False
        self.events[key] = deepcopy(event)
        return True

    def pending(self):
        return [deepcopy(self.events[key]) for key in sorted(self.events)]


def _integer(value, name, minimum):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f'{name} 必须是 >= {minimum} 的整数')
    return value


def normalize_event(event):
    """Return a validated, canonical notification.

    A notification is identified by ``(source, epoch, sequence)``.  Content
    freshness is *not* identified by that sequence: it is identified by the
    per-resource ``revision``.  Keeping the two concepts in the same record is
    what lets a late, low-revision update be rejected without inventing a new
    ordering rule.
    """

    if not isinstance(event, dict):
        raise ValueError('事件必须是对象')
    source = event.get('source')
    epoch = event.get('epoch')
    resource = event.get('resource')
    if not isinstance(source, str) or not source:
        raise ValueError('事件缺少有效来源')
    if not isinstance(epoch, str) or not epoch:
        raise ValueError('事件缺少有效代际')
    if not isinstance(resource, str) or not resource:
        raise ValueError('事件缺少有效资源标识')
    sequence = _integer(event.get('sequence'), '序列号', 1)
    revision = _integer(event.get('revision'), '资源修订号', 0)
    operation = event.get('operation')
    if operation is None and event.get('deleted') is True:
        operation = 'delete'
    if operation not in OPERATIONS:
        raise ValueError('事件操作必须是 upsert 或 delete')
    if operation == 'delete':
        value = None
    else:
        value = event.get('value')
    return dict(
        source=source,
        epoch=epoch,
        sequence=sequence,
        resource=resource,
        revision=revision,
        operation=operation,
        value=value,
    )


def normalize_item(item):
    """Validate one resource item returned by a catalog scan."""

    if not isinstance(item, dict):
        raise ValueError('目录条目必须是对象')
    resource = item.get('resource')
    if not isinstance(resource, str) or not resource:
        raise ValueError('目录条目缺少有效资源标识')
    revision = _integer(item.get('revision'), '资源修订号', 0)
    operation = item.get('operation')
    if operation is None:
        operation = 'delete' if item.get('deleted') is True else 'upsert'
    if operation not in OPERATIONS:
        raise ValueError('目录条目操作必须是 upsert 或 delete')
    value = None if operation == 'delete' else item.get('value')
    return dict(resource=resource, revision=revision, operation=operation, value=value)


def epoch_sort_key(epoch):
    """Natural sort key for provider epoch labels (e1 < e2 < e10).

    Epoch labels are opaque to us, but providers conventionally advance them
    monotonically.  Using a deterministic natural ordering means a late
    notification from an older epoch cannot become the "active" epoch merely
    because it arrived last.
    """

    parts = []
    number = ''
    text = ''
    for character in str(epoch):
        if character.isdigit():
            if text:
                parts.append((0, text))
                text = ''
            number += character
        else:
            if number:
                parts.append((1, int(number)))
                number = ''
            text += character
    if number:
        parts.append((1, int(number)))
    if text:
        parts.append((0, text))
    return tuple(parts)


def collect(payload):
    """Legacy helper: turn a notification payload into import envelopes."""

    inbox = Inbox()
    for event in payload['events']:
        inbox.receive(event)
    return [dict(key=f"{event['source']}:{event['epoch']}:{event['sequence']}", event=event) for event in inbox.pending()]
