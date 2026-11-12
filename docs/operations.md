# 本地运行说明

## 启动顺序

先运行单元测试，再执行演示命令。演示从干净临时目录发起真实回环 HTTP 请求并
处理响应，不需要外部站点。

```bash
python3 -m compileall -q collector
python3 -m unittest discover -s tests -v
python3 -m collector demo
```

`demo` 的退出码非零表示收敛断言失败。它不会只打印 webhook 状态，而是打印每个
阶段的资源目录、来源+代际水位、缺口状态、补拉尝试和最终证据。

## 分进程采集

接收、处理、修复可以各自启动：

```bash
python3 -m collector receive --events fixtures/events.json --db var/records.sqlite3
python3 -m collector process --db var/records.sqlite3
python3 -m collector repair --catalog http://127.0.0.1:9000 --db var/records.sqlite3
python3 -m collector status --db var/records.sqlite3
python3 -m collector gaps --db var/records.sqlite3
```

- `receive` 只登记通知，重复投递返回重复计数；同一来源+代际+序列的冲突内容
  会被拒绝。
- `process` 每次只在一个事务里应用一条通知。进程中断后，未提交的通知仍为
  `pending`，再次运行会接着处理。
- `repair` 对每个 `open` 缺口发起一次范围查询。没有 `--gap` 时处理全部开放缺口。
  只有 `complete=true`、非空 `terminal` 且返回范围覆盖请求范围的响应才会清除
  缺口。
- `sync` 依次执行接收、处理、修复，并输出健康状态、缺口和修复结果。

多个进程可以同时连接同一个 SQLite 数据库。数据库启用 WAL，写事务使用
`BEGIN IMMEDIATE`；并发处理同一批通知时，修订号比较保证最终资源视图一致，
事件也不会因重复处理而重复发布。

## 查询服务

```bash
python3 -m collector serve --db var/records.sqlite3 --port 8080
```

监听回环地址：

- `GET /records`：旧导入路径的当前记录；
- `GET /resources`：带墓碑的当前资源目录，可用 `?source=` 过滤；
- `GET /health`：每个来源的健康状态，可用 `?source=` 过滤；
- `GET /gaps`：缺口及每次修复尝试，可用 `?status=open&source=&epoch=` 过滤；
- `GET /state`：上述资源、健康、缺口的合并快照。

## 目录查询协议

目录客户端请求：

```text
GET <base>/repair?source=<source>&epoch=<epoch>&start=<start>&end=<end>
```

响应为一个 JSON 对象：

```json
{
  "source": "a",
  "epoch": "e1",
  "range": [2, 4],
  "complete": true,
  "terminal": "end-1",
  "items": [
    {"resource": "r", "revision": 4, "deleted": true}
  ]
}
```

`items` 不携带序列号，内容仍按 `(source, resource, revision)` 应用；条目的
`deleted=true` 或 `operation=delete` 会留下墓碑。`complete=false`、HTTP 失败、
`terminal` 缺失或 `range` 未覆盖请求范围时，补拉尝试会保留在证据表中，但缺口
不会关闭。

## 数据与备份

常规导入失败时返回非零退出码，事务中的原文和记录会一起回滚。
事件表、资源表、水位、缺口、覆盖记录和修复尝试都在同一个 SQLite 文件中。
需要长期保留证据时，请备份整个数据库。

本地响应服务支持状态码、响应头、文本或 base64 字节正文；未配置路径返回未找到。
每条路径的脚本独立推进，最后一个响应会重复；可借请求记录核对调用顺序。
数据文件的路径均相对项目根目录，修改工作目录前请确认输入和数据库参数。
