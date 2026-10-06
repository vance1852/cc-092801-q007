# 修复资料交接幂等冲突被吞掉基础平台

本项目是一套可离线运行的 Python 服务端平台，供创新药企业的研发管理、转化医学、商务拓展和基金运营团队管理候选药实验记录、研发证据、协作中心资源、尽调交接通道、交易风险告警与跟进任务。业务状态、角色权限、幂等结果和审计事件保存在 SQLite 中，可在单个 Linux 应用容器内运行。

## 目录

- src/portfolio_ops/：研发中心、尽调通道、研究资源、交接计划和商业情景；
- src/discovery_lab/：研究协议、实验记录、异常排除、分析任务租约和候选结论；
- src/licensing_ops/：管线信息、交易风险告警、跟进工单和资源分配；
- fixtures/：离线验收使用的研究协议与结构化实验记录；
- tests/：领域规则、事务边界、权限、HTTP API 和命令行验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时只依赖 Python 标准库与 SQLite

## 测试

~~~bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
~~~

## 构建检查

~~~bash
python3 -m compileall -q src tests
~~~

## 离线验收

~~~bash
PYTHONPATH=src python3 -m portfolio_ops.acceptance --workspace .
PYTHONPATH=src python3 -m discovery_lab.acceptance --workspace .
PYTHONPATH=src python3 -m licensing_ops.acceptance
~~~

三条命令会在临时 SQLite 数据库中完成研发中心与交接通道登记、研究资源分配、候选药证据分析及交易风险处置，不访问外部网络。

## HTTP 服务

~~~bash
PYTHONPATH=src python3 -m portfolio_ops.api --database portfolio.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m discovery_lab.api --database discovery.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m licensing_ops.api --database licensing.sqlite3 --host 127.0.0.1 --port 8082
~~~

服务提供浏览器无关的 JSON 接口和健康检查。进程重启后可以继续读取 SQLite 中的业务状态与审计历史。

## 交接幂等控制

候选项目从发现评估交接到组合执行时，调用方在 `POST /handoffs` 提交候选项目、版本与关键参数并携带幂等键：

- 首次请求在单个 SQLite 事务中创建交接单、执行任务、资源预留与审计事件；
- 完全相同的重试返回首次保存的结果，不重复产生任务、资源预留或审计事件，进程重启后仍然有效；
- 同一幂等键对应不同候选项目、版本或关键参数时返回 409，并把包含字段级差异摘要、请求方和时间的冲突记录写入 `handoff_conflicts` 表与审计链。

运营人员可通过 `GET /handoffs/{handoff_id}` 查看交接结果，通过 `GET /handoff_conflicts`（支持 `idempotency_key`、`candidate_id` 过滤）或 `GET /handoff_conflicts/{conflict_id}` 查询冲突记录。
