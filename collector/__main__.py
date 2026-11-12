import argparse
import json
from pathlib import Path
import tempfile

from .domain import collect
from .storage import Store
from .transport import CatalogClient


def _print(value):
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def main():
    parser = argparse.ArgumentParser(description='本地采集、缺口修复与凭据查看')
    parser.add_argument('command', choices=[
        'demo', 'import', 'show', 'serve',
        'receive', 'process', 'repair', 'sync', 'status', 'gaps',
    ])
    parser.add_argument('--input', default='fixtures/sample.json',
                        help='旧导入命令的输入文件')
    parser.add_argument('--events', default='fixtures/events.json',
                        help='通知流文件')
    parser.add_argument('--snapshots', default='fixtures/snapshots.json',
                        help='演示用的本地补拉响应')
    parser.add_argument('--db', default='var/records.sqlite3')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--catalog', default=None, help='目录查询服务基地址')
    parser.add_argument('--gap', type=int, default=None, help='只修复指定缺口')
    parser.add_argument('--source', default=None)
    parser.add_argument('--epoch', default=None)
    parser.add_argument('--status', default=None, help='按缺口状态过滤')
    arguments = parser.parse_args()

    if arguments.command == 'serve':
        from .api import serve
        serve(arguments.db, arguments.port)
        return

    if arguments.command == 'demo':
        from .demo import run_demo
        with tempfile.TemporaryDirectory() as directory:
            result = run_demo(
                events_path=arguments.events,
                snapshots_path=arguments.snapshots,
                database_path=Path(directory) / 'demo.sqlite3',
            )
        _print(result)
        if not result['converged']:
            raise SystemExit('演示未收敛: ' + json.dumps(result['assertions'], ensure_ascii=False))
        return

    Path(arguments.db).parent.mkdir(parents=True, exist_ok=True)
    store = Store(arguments.db)
    try:
        if arguments.command == 'import':
            raw = Path(arguments.input).read_bytes()
            store.ingest(str(Path(arguments.input)), raw, collect(json.loads(raw)))
            _print(store.records())
        elif arguments.command == 'show':
            _print(store.records())
        elif arguments.command == 'receive':
            payload = json.loads(Path(arguments.events).read_text(encoding='utf-8'))
            results = store.receive_events(payload['events'])
            _print(dict(
                received=sum(1 for item in results if item),
                duplicates=sum(1 for item in results if not item),
                open_gaps=store.open_gaps(),
            ))
        elif arguments.command == 'process':
            _print(dict(processed=store.process_pending()))
        elif arguments.command == 'status':
            _print(dict(health=store.source_health(arguments.source),
                        gaps=store.gaps(status=arguments.status,
                                        source=arguments.source,
                                        epoch=arguments.epoch)))
        elif arguments.command == 'gaps':
            _print(store.gaps(status=arguments.status,
                              source=arguments.source,
                              epoch=arguments.epoch))
        elif arguments.command == 'repair':
            if not arguments.catalog:
                raise SystemExit('repair 需要 --catalog 目录查询地址')
            catalog = CatalogClient(arguments.catalog)
            if arguments.gap is None:
                _print(store.repair_open_gaps(catalog))
            else:
                found = [gap for gap in store.gaps() if gap['id'] == arguments.gap]
                if not found:
                    raise SystemExit('缺口不存在')
                gap = found[0]
                status, url, payload = catalog.fetch(
                    gap['source'], gap['epoch'], gap['start_seq'], gap['end_seq'])
                _print(store.repair_gap(gap['id'], payload, http_status=status, url=url))
        elif arguments.command == 'sync':
            raw = Path(arguments.events).read_text(encoding='utf-8')
            results = store.receive_events(json.loads(raw)['events'])
            processed = store.process_pending()
            repaired = []
            if arguments.catalog:
                repaired = store.repair_open_gaps(CatalogClient(arguments.catalog))
            _print(dict(received=sum(1 for item in results if item),
                        duplicates=sum(1 for item in results if not item),
                        processed=processed,
                        repairs=repaired,
                        health=store.source_health(),
                        gaps=store.gaps()))
        else:  # pragma: no cover - argparse already restricts the choices
            raise SystemExit('未知命令')
    finally:
        store.close()


if __name__ == '__main__':
    main()
