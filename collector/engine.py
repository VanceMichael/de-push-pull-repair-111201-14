"""A small facade over :class:`collector.storage.Store`.

Receipt, application and repair can all use the same ``Collector`` from
different processes; each call opens its own SQLite transaction.  The facade
is mostly for callers that want one object instead of wiring the store and
catalog client together.
"""

from .storage import Store
from .transport import CatalogClient


class Collector:
    def __init__(self, database, catalog=None):
        self.store = database if isinstance(database, Store) else Store(database)
        self.catalog = catalog

    def receive(self, event):
        return self.store.receive_event(event)

    def receive_events(self, events):
        return self.store.receive_events(events)

    def process(self, limit=None):
        return self.store.process_pending(limit=limit)

    def repair(self, gap_id, payload, http_status=200, url=None):
        return self.store.repair_gap(gap_id, payload, http_status=http_status, url=url)

    def repair_open(self):
        if self.catalog is None:
            raise ValueError('未配置目录查询客户端')
        return self.store.repair_open_gaps(self.catalog)

    def health(self, source=None):
        return self.store.source_health(source)

    def gaps(self, status=None, source=None, epoch=None):
        return self.store.gaps(status=status, source=source, epoch=epoch)

    def resources(self, source=None):
        return self.store.resources(source)

    def close(self):
        self.store.close()


CollectionEngine = Collector
SyncEngine = Collector
