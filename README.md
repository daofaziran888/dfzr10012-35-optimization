# 实时流数据处理中心

面向实时数据处理团队的流式计算与管理中心。覆盖流数据源接入、流处理算子编排、窗口计算配置、状态后端管理、检查点恢复、反压监控、延迟追踪与流作业运维的完整实时计算链路。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流数据源接入 | `streamin` | 流数据源 | 源编号、源名称、消息队列 |
| 流处理算子 | `operator` | 算子定义 | 算子编号、算子名称、输入流 |
| 窗口计算配置 | `window` | 窗口定义 | 窗口编号、数据流名称、窗口类型 |
| 状态后端管理 | `state` | 后端配置 | 配置编号、后端类型、存储路径 |
| 检查点恢复 | `checkpoint` | 检查点记录 | 记录编号、流作业名、检查点ID |
| 反压监控面板 | `backpress` | 反压指标 | 指标编号、流作业名、反压级别 |
| 延迟追踪管理 | `latency` | 延迟记录 | 记录编号、流作业名、端到端延迟 |
| 流作业运维 | `jobops` | 作业实例 | 实例编号、作业名称、运行节点 |
| 流数据输出端 | `sink` | 输出配置 | 配置编号、输出类型、目标地址 |
| 流数据格式定义 | `schema` | 格式定义 | 定义编号、流名称、字段名称 |
| 水位线管理 | `watermark` | 水位线配置 | 配置编号、流名称、时间字段 |
| 并行度调优 | `parallel` | 调优记录 | 记录编号、流算子名、当前并行度 |
| 流作业指标采集 | `metric` | 指标采集器 | 采集器编号、作业名称、指标名称 |
| 流告警规则 | `alarm` | 告警规则 | 规则编号、监控指标、告警条件 |
| 自定义函数库 | `udf` | 函数定义 | 函数编号、函数名称、输入参数 |
| 流处理日志中心 | `log` | 日志记录 | 记录编号、流作业名、日志级别 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 日志归档游标迁移（行号 → 事件时间偏移）

流处理日志中心（`log`）的归档游标已从旧行号（`row-101/205/309`）迁移到事件时间
偏移（`event-offset=<毫秒时间戳>`），核心实现在：

- `backend/app/services/log_archive.py`：归档迁移引擎与异常归档数据索引（单例，
  迁移入口与消费入口共用同一份索引）
- `backend/app/routers/log_archive.py`：迁移、续做、消费、旧游标解析、导入/导出接口
- `frontend/src/views/log/ArchivePanel.tsx`：迁移操作与索引核对面板

关键约束：

- 旧行号游标必须映射到**唯一**事件时间偏移后才能继续读取，映射表在迁移完成后冻结，
  历史归档记录按原映射兼容（`row_to_offset_mapping` + `mapping_frozen`）。
- 异常归档数据索引的每条记录都保留三个游标字段：`event_offset`（事件偏移）、
  `processed_cursor`（已处理游标）、`resume_position`（续做位置），并附带日志内容、
  异常类型、处理状态与事件时间校验结论。
- 同一归档的并发迁移只生效一次：状态机 `未迁移 → 迁移中 → 已中断/已完成`，终态唯一；
  中断后只允许走续做入口，从 `resume_position` 继续，不重读已归档区间，已完成文件保留。
- 旧行号与新偏移指向不同位置产生冲突时，只承认来源明确的唯一索引（冲突记录在
  `conflicts`）；导入/导出/上传/下载逐项核对内容与 SHA-256 checksum。
- 验收样例：LOG-0001~LOG-0003 对应 `row-101/205/309`，迁移在 `row-205` 中断后
  续做位置为 `row-206`，续做只补归档 `row-309`。

验收脚本（纯标准库，`-I` 隔离运行）：

```bash
cd backend
.venv/bin/python -I scripts/verify_log_archive.py        # 服务级规则验收（36 项）
.venv/bin/python -I scripts/verify_log_archive_http.py   # 真实 uvicorn 接口验收（18 项）
```

领域：数据工程
子方向：数仓与数据湖
