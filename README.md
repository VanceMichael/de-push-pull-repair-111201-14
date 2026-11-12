# 事件通知与目录拉取·缺口修复

面向合作资料接收人员的 Python 采集组件：把可能重复、失序、断档的变更通知，
与按范围查询的资源目录连接起来，最终收敛到同一份“当前资源目录”，而不是停在
“webhook 返回 200”。

输入样本都是本地合成数据，example 域名只表示来源身份，不需联网访问。
使用 Python 3.11，依赖仅为标准库，SQLite 随解释器提供。

## 安装与运行

从项目根目录执行：

```bash
python3 -m compileall -q collector
python3 -m unittest discover -s tests -v
python3 -m collector demo
```

`demo` 使用 `fixtures/events.json`（通知流）和 `fixtures/snapshots.json`
（本地补拉响应），启动真实回环 HTTP 目录，逐项打印请求、水位、缺口证据、
墓碑和最终资源目录。每次演示使用独立临时目录，结束后清理。

常规采集可跨进程、跨重启继续：

```bash
python3 -m collector receive --events fixtures/events.json --db var/records.sqlite3
python3 -m collector process --db var/records.sqlite3
python3 -m collector repair --catalog http://127.0.0.1:9000 --db var/records.sqlite3
python3 -m collector status --db var/records.sqlite3
python3 -m collector gaps --db var/records.sqlite3
```

`receive` 只登记通知；`process` 才把通知应用到当前资源目录；`repair` 用目录
查询客户端按缺口范围补拉。三者可以分别运行，也可以 `sync` 串起来：

```bash
python3 -m collector sync --events fixtures/events.json \
  --catalog http://127.0.0.1:9000 --db var/records.sqlite3
```

旧导入与只读查询仍保留：

```bash
python3 -m collector import --input fixtures/sample.json --db var/records.sqlite3
python3 -m collector show --db var/records.sqlite3
python3 -m collector serve --db var/records.sqlite3 --port 8080
```

查询服务监听回环地址，`GET /records`（旧导入记录）、`/resources`、
`/health`、`/gaps`、`/state` 返回 SQLite 中的实际状态；按 Ctrl-C 停止。

## 收敛规则

- **接收不等于应用。** 通知先以 `pending` 落入 `events`；`process_pending`
  在同一事务内应用内容并标记 `applied`。进程在提交前崩溃时，重启后仍会从
  `pending` 继续。
- **内容新旧看资源修订号。** 同一 `(source, resource)` 只接受更大的
  `revision`；`revision` 相同时删除优先，旧更新无法把已撤下的资源重新发布。
- **缺口看来源+代际序列。** 每个 `(source, epoch)` 单独维护“从 1 开始连续”
  的水位 `contiguous_sequence`，绝不拿全局最高序列充当连续水位。水位上方
  已观测到的范围形成缺口 `[contiguous_sequence+1, highest_observed]`。
- **只有覆盖凭据能清除缺口。** 补拉响应必须同时满足 `complete=true`、
  非空 `terminal` 凭据，且返回范围覆盖请求范围，才写入 `coverages` 并把缺口
  标为 `covered`。失败、截断、无凭据或范围不覆盖的响应只登记
  `repair_attempts`，缺口保持 `open`。截断响应中的有效条目仍可按修订规则应用，
  但不能成为“已扫完”的证据。
- **删除留墓碑。** `resources` 保留 `operation=delete`、`revision` 和来源/
  代际/序列。墓碑不是“当前发布记录”，但会挡住旧通知和晚到的低修订补拉；
  只有当更高修订的 upsert 到来时才合法复活。
- **代际隔离。** 水位、缺口、覆盖记录都以 `(source, epoch)` 为键。新代际开始后，
  旧代际的迟到通知只更新旧代际自己的水位，不会移动新代际的连续水位；
  活跃代际按代际标签的自然顺序（e1 < e2 < e10）确定。
- **并发与重启。** 所有写操作使用 SQLite `BEGIN IMMEDIATE` 事务。接收、处理、
  修复可由不同进程同时调用；修订号比较使任意处理顺序都收敛，事务回滚使未应用
  事件保持 `pending`。

## 健康状态与证据

`status` / `GET /health` 按来源给出：

- `status`：`healthy`（无缺口、无积压）、`backlog`（尚有未应用事件）、
  `degraded`（有未证明的缺口）；
- `active_epoch` 和各代际的 `contiguous_sequence`、`highest_observed`、
  `pending_events`、`open_gaps`、`coverages`；
- `published`、`tombstones`、最近事件/修复时间。

`gaps` / `GET /gaps` 给出每个缺口的历史记录：请求范围、缺失序列、
所有 `repair_attempts`（HTTP 状态、`complete`、`terminal`、条目应用结果、
失败原因），以及最终 `evidence`（覆盖凭据、覆盖范围、尝试编号和时间）。
被通知补齐的缺口也会保留，证据方法为 `notifications`。

## 演示覆盖的三个场景

1. **删除先到、旧更新后到**：先接收 `e1/4` 删除，再接收 `e1/2` 旧更新。
   修订 4 的墓碑始终保留，`r` 不出现在发布目录。
2. **断档后补拉**：`e1/3` 缺失形成缺口 `[2,4]`。第一次补拉 `complete=false`
   被记为 `truncated`，缺口仍打开；第二次补拉带 `terminal=end-1`，缺口转为
   `covered`，并记录两次尝试的完整证据。
3. **序列重置**：`e2/1` 开始新代际后，迟到的 `e1/2` 只更新 `e1` 水位；
   活跃代际仍是 `e2`，`e2` 的连续水位保持 1。最终目录为：`a:r` 墓碑修订 4，
   `a:s` 已发布修订 1，两个代际均无开放缺口。
