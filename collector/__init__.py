"""Convergent notification/catalog collector.

Public building blocks are re-exported for callers that prefer a single import
while the implementation remains split by responsibility:

* :mod:`collector.domain` -- validation, revision/epoch ordering.
* :mod:`collector.storage` -- SQLite inbox, watermarks, gaps and repair ledger.
* :mod:`collector.transport` -- HTTP client, catalog query client and local
  scripted response server.
* :mod:`collector.engine` -- a small facade tying the pieces together.
"""

from .domain import Inbox, collect, epoch_sort_key, normalize_event, normalize_item
from .engine import CollectionEngine, Collector, SyncEngine
from .storage import Store
from .transport import CatalogClient, Client, replay_server

__all__ = [
    'Inbox', 'collect', 'epoch_sort_key', 'normalize_event', 'normalize_item',
    'Store', 'Collector', 'CollectionEngine', 'SyncEngine',
    'CatalogClient', 'Client', 'replay_server',
]
