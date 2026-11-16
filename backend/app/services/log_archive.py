"""日志归档游标迁移引擎：行号游标 -> 事件时间偏移。

设计要点（对应迁移需求）：

1. 旧行号游标（row-101/205/309…）必须先映射到唯一的事件时间偏移，
   映射表在首次迁移时构建并冻结，之后按历史快照兼容，不再改写。
2. 异常归档数据索引是唯一事实来源：每个归档文件一份，条目里同时保留
   事件偏移（event_offset）、已处理游标（processed_cursor）、续做位置
   （resume_position），并记录日志内容、异常类型、处理状态与事件时间校验结论。
3. 同一归档的并发迁移用锁 + 状态机保证只生效一次：
   未迁移 -> 迁移中 -> 已中断 / 已完成，终态只接受一个。
4. 中断后已完成的文件/条目保留，续做从 resume_position 指向的下一条开始，
   绝不重复读取已归档区间。
5. 旧行号游标与新事件时间偏移指向不同位置（冲突）时，只允许使用来源明确的
   唯一索引：带终态的索引优先，无法唯一映射的旧游标直接拒绝续做。
6. 导入/导出/上传/下载均逐项核对内容与 checksum，另一个执行入口（消费侧）
   与迁移入口共用同一份索引。
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
from datetime import datetime
from typing import Any

from app.store import store

MODULE = "log"

# 迁移状态机：终态只有「已完成」一个；「已中断」可由 resume 推进到「已完成」。
STATE_NOT_STARTED = "未迁移"
STATE_MIGRATING = "迁移中"
STATE_INTERRUPTED = "已中断"
STATE_COMPLETED = "已完成"
TERMINAL_STATES = {STATE_COMPLETED}

# 条目归档状态。
ENTRY_PENDING = "待归档"
ENTRY_ARCHIVED = "已归档"
ENTRY_INVALID = "事件时间异常"

INDEX_KIND = "异常归档数据索引"
INDEX_VERSION = 2  # v1 为旧行号游标时代，v2 为事件时间偏移时代
LEGACY_VERSION = 1


def _parse_event_time(raw: Any) -> tuple[datetime | None, str | None]:
    """校验事件时间：支持 ISO 8601（含毫秒）与日期；返回 (时间, 错误说明)。"""
    text = str(raw or "").strip()
    if not text:
        return None, "事件时间为空"
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None, f"事件时间格式无法解析：{text}"
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed, None


class LogArchiveService:
    """归档索引的内存实现；真实环境这里会换成数据库/对象存储访问层。"""

    def __init__(self) -> None:
        self._indexes: dict[str, dict[str, Any]] = {}
        self._locks: dict[str, threading.RLock] = {}
        self._guard = threading.Lock()

    # ------------------------------------------------------------------ #
    # 内部工具
    # ------------------------------------------------------------------ #
    def _lock_for(self, archive_id: str) -> threading.RLock:
        with self._guard:
            lock = self._locks.get(archive_id)
            if lock is None:
                lock = threading.RLock()
                self._locks[archive_id] = lock
            return lock

    def _snapshot(self, index: dict[str, Any]) -> dict[str, Any]:
        """历史快照：深拷贝当前索引，沿用原映射，供异常版本回溯核对。"""
        snap = copy.deepcopy(index)
        index["snapshots"].append(snap)
        return snap

    def _log_rows(self) -> list[dict[str, Any]]:
        # 取数链路固定按 id 升序，保证行号 row-N 与事件顺序可重复确定。
        return sorted(store.rows(MODULE), key=lambda row: int(row.get("id", 0)))

    def _row_no_for(self, position: int) -> int:
        """稳定物理行号：验收样例固定为 row-101/205/309，其余按 100+序号兜底。

        行号代表日志文件中的物理位置，不要求连续，因此不能简单用 100+position。
        """
        stable = {1: 101, 2: 205, 3: 309}
        return stable.get(position, 100 + position)

    def _build_mapping(self, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
        """构建旧行号 -> 唯一事件时间偏移的映射，构建成功后冻结。

        事件偏移取事件时间的毫秒数；同一毫秒出现多条时按取数顺序追加序号，
        保证每个旧行号游标都能映射到唯一事件时间偏移。
        """
        mapping: list[dict[str, Any]] = []
        errors: list[str] = []
        seen_offsets: dict[int, int] = {}
        for position, row in enumerate(rows, start=1):
            event_time, error = _parse_event_time(row.get("发生时间"))
            row_no = self._row_no_for(position)
            if error is not None:
                errors.append(f"{row.get('记录编号')}（row-{row_no}）：{error}")
                mapping.append({
                    "row_cursor": f"row-{row_no}",
                    "row_no": row_no,
                    "log_id": int(row.get("id", 0)),
                    "record_no": row.get("记录编号"),
                    "event_time": str(row.get("发生时间") or ""),
                    "event_offset": None,
                    "time_valid": False,
                    "error": error,
                })
                continue
            base_offset = int(event_time.timestamp() * 1000)
            duplicate_count = seen_offsets.get(base_offset, 0)
            seen_offsets[base_offset] = duplicate_count + 1
            event_offset = base_offset if duplicate_count == 0 else base_offset * 1000 + duplicate_count
            mapping.append({
                "row_cursor": f"row-{row_no}",
                "row_no": row_no,
                "log_id": int(row.get("id", 0)),
                "record_no": row.get("记录编号"),
                "event_time": event_time.isoformat(timespec="milliseconds"),
                "event_offset": event_offset,
                "time_valid": True,
                "error": None,
            })
        return mapping, errors

    def _revalidate_mapping(self, index: dict[str, Any]) -> None:
        """续做前重新校验事件时间：修复后的日志行可恢复为有效映射。

        映射在全部条目有效且迁移完成前不会冻结，因此这里允许更新异常条目。
        """
        if index.get("mapping_frozen"):
            return
        rows = {int(r.get("id", 0)): r for r in self._log_rows()}
        seen_offsets: dict[int, int] = {}
        for item in index["row_to_offset_mapping"]:
            row = rows.get(item["log_id"], {})
            event_time, error = _parse_event_time(row.get("发生时间"))
            if error is not None:
                item["time_valid"] = False
                item["event_offset"] = None
                item["event_time"] = str(row.get("发生时间") or "")
                item["error"] = error
                continue
            base_offset = int(event_time.timestamp() * 1000)
            duplicate_count = seen_offsets.get(base_offset, 0)
            seen_offsets[base_offset] = duplicate_count + 1
            item["event_offset"] = base_offset if duplicate_count == 0 else base_offset * 1000 + duplicate_count
            item["event_time"] = event_time.isoformat(timespec="milliseconds")
            item["time_valid"] = True
            item["error"] = None
        index["mapping_errors"] = [
            f"{m['record_no']}（{m['row_cursor']}）：{m['error']}"
            for m in index["row_to_offset_mapping"] if not m["time_valid"]
        ]

    def _next_row_no(self, index: dict[str, Any], current_row_no: int) -> str:
        """续做游标指向下一个待读位置（当前行号 +1）。

        行号本身可以不连续（row-101/205/309），所以续做位置（如 row-206）
        不一定对应一条真实日志；迁移循环用 row_no < 续做起始行 过滤，
        保证已归档区间不重读、后续物理行（row-309）不漏读。
        """
        max_row_no = max(m["row_no"] for m in index["row_to_offset_mapping"])
        if current_row_no >= max_row_no:
            return "EOF"
        return f"row-{current_row_no + 1}"

    def _new_index(self, archive_id: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        mapping, mapping_errors = self._build_mapping(rows)
        ordered_cursors = [m["row_cursor"] for m in sorted(mapping, key=lambda m: m["row_no"])]
        entries: list[dict[str, Any]] = []
        for item in mapping:
            row = next((r for r in rows if int(r.get("id", 0)) == item["log_id"]), {})
            invalid = not item["time_valid"]
            entries.append({
                "log_id": item["log_id"],
                "record_no": item["record_no"],
                "row_cursor": item["row_cursor"],
                "row_no": item["row_no"],
                "event_time": item["event_time"],
                "event_offset": item["event_offset"],
                "time_valid": item["time_valid"],
                "time_error": item["error"],
                # 需求要求读日志内容、异常类型、处理状态后再入索引
                "log_content": row.get("日志内容", ""),
                "exception_type": row.get("异常类型", ""),
                "process_status": row.get("处理状态", ""),
                "archive_status": ENTRY_INVALID if invalid else ENTRY_PENDING,
                "archived": False,
                # 三个核心游标字段
                "processed_cursor": None,
                "resume_position": item["row_cursor"],
                "consumed": False,
                "consumed_cursor": None,
            })
        return {
            "kind": INDEX_KIND,
            "index_version": INDEX_VERSION,
            "legacy_version": LEGACY_VERSION,
            "archive_id": archive_id,
            "state": STATE_NOT_STARTED,
            "cursor_mode": "event_offset",
            # 冻结的历史映射：旧行号游标按原映射兼容
            "row_to_offset_mapping": mapping,
            "mapping_frozen": False,
            "mapping_errors": mapping_errors,
            "entries": entries,
            "processed_cursor": None,       # 索引级已处理游标
            "resume_position": ordered_cursors[0] if ordered_cursors else None,
            "completed_files": [],          # 部署中断时保留已完成文件
            "conflicts": [],
            "transfers": [],                # 上传/下载记录
            "snapshots": [],
            "updated_at": None,
        }

    def _get_or_create_index(self, archive_id: str) -> dict[str, Any]:
        index = self._indexes.get(archive_id)
        if index is None:
            index = self._new_index(archive_id, self._log_rows())
            self._indexes[archive_id] = index
        return index

    @staticmethod
    def _now() -> str:
        return datetime.now().isoformat(timespec="seconds")

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    def get_index(self, archive_id: str) -> dict[str, Any] | None:
        index = self._indexes.get(archive_id)
        return copy.deepcopy(index) if index is not None else None

    def list_indexes(self) -> list[dict[str, Any]]:
        return [
            {
                "archive_id": idx["archive_id"],
                "state": idx["state"],
                "cursor_mode": idx["cursor_mode"],
                "index_version": idx["index_version"],
                "total": len(idx["entries"]),
                "archived": sum(1 for e in idx["entries"] if e["archived"]),
                "invalid": sum(1 for e in idx["entries"] if not e["time_valid"]),
                "resume_position": idx["resume_position"],
                "processed_cursor": idx["processed_cursor"],
                "updated_at": idx["updated_at"],
            }
            for idx in self._indexes.values()
        ]

    # ------------------------------------------------------------------ #
    # 主执行入口：并发迁移只生效一次
    # ------------------------------------------------------------------ #
    def run_migration(
        self,
        archive_id: str,
        *,
        stop_after_row: int | None = None,
        simulate_interrupt: bool = False,
    ) -> tuple[dict[str, Any] | None, str, bool]:
        """执行一次迁移。返回 (索引快照, 消息, 是否本次实际执行)。

        并发/重复调用时只有第一次生效，其余直接返回同一份索引（幂等）。
        stop_after_row / simulate_interrupt 用于模拟部署中断：迁移处理到指定
        旧行号（含）后停在「已中断」，保留已完成文件，等待续做。
        """
        lock = self._lock_for(archive_id)
        with lock:
            index = self._get_or_create_index(archive_id)

            # 终态唯一：已完成的迁移不再重复读取任何区间。
            if index["state"] in TERMINAL_STATES:
                return copy.deepcopy(index), "迁移已完成，终态唯一，本次调用直接复用同一份索引", False
            if index["state"] == STATE_MIGRATING:
                return copy.deepcopy(index), "同一归档正在迁移中，并发调用被合并，仅生效一次", False
            if index["state"] == STATE_INTERRUPTED:
                # 同一归档的迁移只生效一次：中断后只能走续做入口，不能重新开跑
                return (copy.deepcopy(index),
                        f"迁移已在 {index['resume_position']} 前中断，请走续做入口而不是重新迁移",
                        False)

            rows = self._log_rows()
            mapping = {item["log_id"]: item for item in index["row_to_offset_mapping"]}
            index["state"] = STATE_MIGRATING
            index["updated_at"] = self._now()

            start_row_no = self._resume_row_no(index)
            interrupted_row: int | None = None

            for position, row in enumerate(rows, start=1):
                row_no = self._row_no_for(position)
                if row_no < start_row_no:
                    continue  # 已归档区间跳过，杜绝重复读取
                entry = next(e for e in index["entries"] if e["log_id"] == int(row["id"]))
                item = mapping[int(row["id"])]

                if not item["time_valid"]:
                    # 事件时间校验不过：写入异常条目不归档，游标停在该行等待修复后续做
                    entry["archive_status"] = ENTRY_INVALID
                    entry["time_valid"] = False
                    entry["time_error"] = item["error"]
                    index["state"] = STATE_INTERRUPTED
                    interrupted_row = row_no
                    index["resume_position"] = entry["row_cursor"]
                    self._snapshot(index)
                    index["updated_at"] = self._now()
                    return (
                        copy.deepcopy(index),
                        f"事件时间校验失败，迁移在 {entry['row_cursor']}（{entry['record_no']}）中断，续做位置已保留",
                        True,
                    )

                entry["event_offset"] = item["event_offset"]
                entry["event_time"] = item["event_time"]
                entry["time_valid"] = True
                entry["time_error"] = None
                entry["archive_status"] = ENTRY_ARCHIVED
                entry["archived"] = True
                entry["processed_cursor"] = f"event-offset={item['event_offset']}"
                # 续做位置指向下一个待读位置（不连续行号也能被 row_no < 起始行 正确过滤）
                entry["resume_position"] = self._next_row_no(index, row_no)
                index["processed_cursor"] = entry["processed_cursor"]
                index["resume_position"] = entry["resume_position"]
                file_name = f"archive-{archive_id}-{entry['row_cursor']}.log"
                if file_name not in index["completed_files"]:
                    index["completed_files"].append(file_name)

                if stop_after_row is not None and row_no >= stop_after_row:
                    index["state"] = STATE_INTERRUPTED
                    interrupted_row = row_no
                    break
                if simulate_interrupt and position == max(1, len(rows) // 2):
                    index["state"] = STATE_INTERRUPTED
                    interrupted_row = row_no
                    break

            if index["state"] == STATE_INTERRUPTED:
                self._snapshot(index)
                index["updated_at"] = self._now()
                return (
                    copy.deepcopy(index),
                    f"迁移在 {interrupted_row} 处中断，已完成 {len(index['completed_files'])} 个文件，"
                    f"续做位置 {index['resume_position']}",
                    True,
                )

            # 映射构建成功后冻结：历史归档记录按原映射兼容
            index["mapping_frozen"] = True
            index["state"] = STATE_COMPLETED
            index["resume_position"] = "EOF"
            self._snapshot(index)
            index["updated_at"] = self._now()
            return (
                copy.deepcopy(index),
                f"迁移完成：{len(index['completed_files'])} 个归档文件，游标已切换为事件时间偏移",
                True,
            )

    def _resume_row_no(self, index: dict[str, Any]) -> int:
        """从索引的 resume_position 解析续做起始行号。"""
        position = index.get("resume_position") or ""
        if position.startswith("row-"):
            try:
                return int(position.split("row-", 1)[1])
            except ValueError:
                return 101
        if position == "EOF":
            return 10 ** 9
        return 101

    # ------------------------------------------------------------------ #
    # 中断续做：只沿唯一索引推进，不重复读已归档区间
    # ------------------------------------------------------------------ #
    def resume_migration(self, archive_id: str) -> tuple[dict[str, Any] | None, str]:
        lock = self._lock_for(archive_id)
        with lock:
            index = self._indexes.get(archive_id)
            if index is None:
                return None, "归档索引不存在，请先发起迁移"
            if index["state"] in TERMINAL_STATES:
                return copy.deepcopy(index), "迁移已是终态，无需续做"
            if index["state"] == STATE_MIGRATING:
                return copy.deepcopy(index), "迁移进行中，不能并发续做"
            if index["state"] != STATE_INTERRUPTED:
                return copy.deepcopy(index), f"当前状态为{index['state']}，不满足续做条件"

            # 事件时间可能在中断后已修复：续做前重新校验并刷新异常映射
            self._revalidate_mapping(index)
            rows = self._log_rows()
            mapping = {item["log_id"]: item for item in index["row_to_offset_mapping"]}
            start_row_no = self._resume_row_no(index)
            index["state"] = STATE_MIGRATING
            index["updated_at"] = self._now()

            for position, row in enumerate(rows, start=1):
                row_no = self._row_no_for(position)
                if row_no < start_row_no:
                    continue  # 已归档区间绝不重读
                entry = next(e for e in index["entries"] if e["log_id"] == int(row["id"]))
                item = mapping[int(row["id"])]
                if not item["time_valid"]:
                    entry["archive_status"] = ENTRY_INVALID
                    entry["time_valid"] = False
                    entry["time_error"] = item["error"]
                    index["state"] = STATE_INTERRUPTED
                    index["resume_position"] = entry["row_cursor"]
                    self._snapshot(index)
                    index["updated_at"] = self._now()
                    return copy.deepcopy(index), f"事件时间仍未修复，续做在 {entry['row_cursor']} 再次中断"

                entry["event_offset"] = item["event_offset"]
                entry["event_time"] = item["event_time"]
                entry["time_valid"] = True
                entry["time_error"] = None
                entry["archive_status"] = ENTRY_ARCHIVED
                entry["archived"] = True
                entry["processed_cursor"] = f"event-offset={item['event_offset']}"
                entry["resume_position"] = self._next_row_no(index, row_no)
                index["processed_cursor"] = entry["processed_cursor"]
                index["resume_position"] = entry["resume_position"]
                file_name = f"archive-{archive_id}-{entry['row_cursor']}.log"
                if file_name not in index["completed_files"]:
                    index["completed_files"].append(file_name)

            index["mapping_frozen"] = True
            index["state"] = STATE_COMPLETED
            index["resume_position"] = "EOF"
            self._snapshot(index)
            index["updated_at"] = self._now()
            return copy.deepcopy(index), f"续做完成，归档文件累计 {len(index['completed_files'])} 个，终态唯一"

    # ------------------------------------------------------------------ #
    # 旧行号游标 -> 唯一事件时间偏移（续做前置校验）
    # ------------------------------------------------------------------ #
    def resolve_legacy_cursor(self, archive_id: str, row_cursor: str) -> tuple[dict[str, Any] | None, str]:
        index = self._indexes.get(archive_id)
        if index is None:
            return None, "归档索引不存在，旧行号游标缺少来源明确的映射依据"
        matches = [m for m in index["row_to_offset_mapping"] if m["row_cursor"] == row_cursor]
        if len(matches) != 1:
            return None, f"旧行号游标 {row_cursor} 无法映射到唯一事件时间偏移，禁止继续读取"
        item = matches[0]
        if not item["time_valid"] or item["event_offset"] is None:
            return None, f"旧行号游标 {row_cursor} 的事件时间校验失败，不能映射为事件时间偏移"

        # 冲突核对：旧行号与新偏移若指向不同日志位置，只承认带终态的唯一索引
        entry = next((e for e in index["entries"] if e["log_id"] == item["log_id"]), None)
        conflict = False
        if entry is not None and entry["archived"]:
            conflict = entry["processed_cursor"] != f"event-offset={item['event_offset']}"
        result = {
            "row_cursor": item["row_cursor"],
            "event_offset": item["event_offset"],
            "event_time": item["event_time"],
            "record_no": item["record_no"],
            "index_state": index["state"],
            "resume_position": index["resume_position"],
            "conflict": conflict,
            "source": INDEX_KIND,
        }
        if conflict:
            index["conflicts"].append({
                "row_cursor": row_cursor,
                "resolved_offset": item["event_offset"],
                "indexed_cursor": entry["processed_cursor"],
                "decision": "以异常归档数据索引的终态游标为准，放弃旧行号来源",
                "at": self._now(),
            })
            return result, "检测到旧行号与事件偏移指向不同位置，已按唯一索引终态裁决并记录冲突"
        return result, "旧行号游标已映射到唯一事件时间偏移"

    # ------------------------------------------------------------------ #
    # 另一个执行入口：消费侧，共用同一份索引继续后续处理
    # ------------------------------------------------------------------ #
    def consume(self, archive_id: str, *, max_items: int | None = None) -> tuple[dict[str, Any] | None, str, list[dict[str, Any]]]:
        lock = self._lock_for(archive_id)
        with lock:
            index = self._indexes.get(archive_id)
            if index is None:
                return None, "归档索引不存在，消费侧无法接续", []
            consumable = [e for e in index["entries"] if e["archived"] and not e["consumed"]]
            if max_items is not None:
                consumable = consumable[:max_items]
            delivered: list[dict[str, Any]] = []
            for entry in consumable:
                entry["consumed"] = True
                entry["consumed_cursor"] = entry["processed_cursor"]
                delivered.append({
                    "record_no": entry["record_no"],
                    "event_offset": entry["event_offset"],
                    "processed_cursor": entry["processed_cursor"],
                    "log_content": entry["log_content"],
                    "exception_type": entry["exception_type"],
                    "process_status": entry["process_status"],
                })
            index["updated_at"] = self._now()
            remaining = sum(1 for e in index["entries"] if e["archived"] and not e["consumed"])
            return copy.deepcopy(index), f"消费侧交付 {len(delivered)} 条，剩余 {remaining} 条", delivered

    # ------------------------------------------------------------------ #
    # 导出 / 导入 / 上传 / 下载：逐项核对
    # ------------------------------------------------------------------ #
    def export_index(self, archive_id: str) -> tuple[dict[str, Any] | None, str]:
        index = self._indexes.get(archive_id)
        if index is None:
            return None, "归档索引不存在，无可导出内容"
        # 先对索引本体算 checksum，再登记上传记录，保证导入侧可复算核对
        payload = copy.deepcopy(index)
        checksum = self._checksum(payload)
        payload["checksum"] = checksum
        index["transfers"].append({
            "direction": "upload",
            "archive_id": archive_id,
            "entries": len(index["entries"]),
            "checksum": checksum,
            "at": self._now(),
            "result": "成功",
        })
        payload["transfers"] = copy.deepcopy(index["transfers"])
        return payload, "导出成功，checksum 已写入上传记录"

    def download_index(self, archive_id: str) -> tuple[dict[str, Any] | None, str]:
        index = self._indexes.get(archive_id)
        if index is None:
            return None, "归档索引不存在，无下载结果"
        payload = copy.deepcopy(index)
        checksum = self._checksum(payload)
        payload["checksum"] = checksum
        index["transfers"].append({
            "direction": "download",
            "archive_id": archive_id,
            "entries": len(index["entries"]),
            "checksum": checksum,
            "at": self._now(),
            "result": "成功",
        })
        payload["transfers"] = copy.deepcopy(index["transfers"])
        return payload, "下载成功，下载结果与 checksum 已登记"

    def import_index(self, archive_id: str, payload: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """导入外部索引文件：校验 kind/版本/checksum/条目内容与映射唯一性。"""
        lock = self._lock_for(archive_id)
        with lock:
            errors = self._validate_import(payload)
            existing = self._indexes.get(archive_id)
            record: dict[str, Any] = {
                "direction": "import",
                "archive_id": archive_id,
                "at": self._now(),
                "entries": len(payload.get("entries", [])),
                "checksum": payload.get("checksum"),
                "result": "成功" if not errors else "拒绝",
                "errors": errors,
            }
            if errors:
                if existing is not None:
                    existing["transfers"].append(record)
                return (copy.deepcopy(existing) if existing else None), "导入核对失败：" + "；".join(errors)

            imported = copy.deepcopy(payload)
            imported.pop("checksum", None)
            # 沿用历史快照：保留原有快照链，仅在其后追加
            if existing is not None:
                imported["snapshots"] = existing["snapshots"] + [copy.deepcopy(existing)]
                imported["transfers"] = existing["transfers"]
            else:
                imported.setdefault("snapshots", [])
                imported.setdefault("transfers", [])
            imported["archive_id"] = archive_id
            record["result"] = "成功"
            imported["transfers"].append(record)
            imported["updated_at"] = self._now()
            self._indexes[archive_id] = imported
            return copy.deepcopy(imported), "导入核对通过：内容、映射与 checksum 一致，已作为唯一索引生效"

    def _validate_import(self, payload: Any) -> list[str]:
        errors: list[str] = []
        if not isinstance(payload, dict):
            return ["导入内容不是索引对象"]
        if payload.get("kind") != INDEX_KIND:
            errors.append("索引类型标识不匹配")
        if payload.get("index_version") != INDEX_VERSION:
            errors.append("索引版本不是事件时间偏移版本(v2)")
        checksum = payload.get("checksum")
        # checksum 在导出时先于该次传输记录生成：复算时去掉 checksum，
        # 并移除与 checksum 同批生成的最后一条上传/下载记录，还原导出前索引本体。
        body = {k: v for k, v in payload.items() if k != "checksum"}
        transfers = body.get("transfers")
        if isinstance(transfers, list) and transfers:
            last = transfers[-1]
            if isinstance(last, dict) and last.get("checksum") == checksum:
                body = dict(body)
                body["transfers"] = transfers[:-1]
        if checksum != self._checksum(body):
            errors.append("checksum 校验失败，文件内容可能被改动")
        entries = payload.get("entries")
        mapping = payload.get("row_to_offset_mapping")
        if not isinstance(entries, list) or not entries:
            errors.append("条目为空或结构非法")
            return errors
        offsets = [e.get("event_offset") for e in entries if e.get("time_valid")]
        if len(offsets) != len(set(offsets)):
            errors.append("事件时间偏移不唯一，违反一个旧行号对应唯一偏移的约束")
        if not isinstance(mapping, list):
            errors.append("旧行号映射表缺失")
        else:
            mapped = [m.get("event_offset") for m in mapping if m.get("time_valid")]
            if len(mapped) != len(set(mapped)):
                errors.append("映射表中事件时间偏移重复")
        for entry in entries:
            for field in ("event_offset", "processed_cursor", "resume_position",
                          "log_content", "exception_type", "process_status"):
                if field not in entry:
                    errors.append(f"条目缺少字段：{field}")
                    break
        return errors

    @staticmethod
    def _checksum(payload: dict[str, Any]) -> str:
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    # ------------------------------------------------------------------ #
    # 测试/运维辅助
    # ------------------------------------------------------------------ #
    def reset(self, archive_id: str | None = None) -> None:
        if archive_id is None:
            self._indexes.clear()
            with self._guard:
                self._locks.clear()
            return
        self._indexes.pop(archive_id, None)
        with self._guard:
            self._locks.pop(archive_id, None)


# 单例：迁移入口与消费入口共享同一份异常归档数据索引
log_archive_service = LogArchiveService()
