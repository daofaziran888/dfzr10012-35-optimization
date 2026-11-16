"""日志归档游标迁移验收脚本（纯标准库，服务级，不依赖 HTTP 客户端）。

运行：backend/.venv/bin/python -I scripts/verify_log_archive.py
"""
from __future__ import annotations

import concurrent.futures
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.log_archive import (  # noqa: E402
    ENTRY_ARCHIVED,
    STATE_COMPLETED,
    STATE_INTERRUPTED,
    log_archive_service,
)
from app.store import store  # noqa: E402

PASS = "PASS"
FAIL = "FAIL"
results: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    results.append((name, bool(condition), detail))
    mark = PASS if condition else FAIL
    print(f"[{mark}] {name}" + (f" —— {detail}" if detail else ""))


def entry_by_record(index: dict, record_no: str) -> dict:
    return next(e for e in index["entries"] if e["record_no"] == record_no)


def main() -> int:
    log_archive_service.reset()
    archive_id = "LOG-ARCHIVE-2026Q3"
    log_rows = sorted(store.rows("log"), key=lambda r: int(r["id"]))
    check("种子数据含 LOG-0001/0002/0003",
          [r["记录编号"] for r in log_rows] == ["LOG-0001", "LOG-0002", "LOG-0003"])

    # ---------- 验收样例：迁移后停在 row-205，后续从 row-206 继续 ----------
    index, message, executed = log_archive_service.run_migration(
        archive_id, stop_after_row=205
    )
    check("迁移实际执行一次", executed, message)
    check("中断状态为「已中断」且保留已完成文件",
          index["state"] == STATE_INTERRUPTED and len(index["completed_files"]) == 2,
          f"state={index['state']}, files={index['completed_files']}")

    e1, e2, e3 = (entry_by_record(index, f"LOG-000{i}") for i in (1, 2, 3))
    check("LOG-0001 对应 row-101 且已归档",
          e1["row_cursor"] == "row-101" and e1["archive_status"] == ENTRY_ARCHIVED)
    check("LOG-0002 对应 row-205 且已归档（停在 row-205）",
          e2["row_cursor"] == "row-205" and e2["archive_status"] == ENTRY_ARCHIVED)
    check("LOG-0003 对应 row-309 尚未归档，不重复读未完成区间",
          e3["row_cursor"] == "row-309" and e3["archived"] is False)
    check("索引续做位置为 row-206", index["resume_position"] == "row-206",
          index["resume_position"])
    check("条目保留事件偏移/已处理游标/续做位置",
          all(e["event_offset"] and e["processed_cursor"] and e["resume_position"]
              for e in (e1, e2)))
    check("已处理游标采用事件时间偏移",
          e1["processed_cursor"].startswith("event-offset=")
          and e2["processed_cursor"].startswith("event-offset="),
          f"{e1['processed_cursor']} / {e2['processed_cursor']}")
    check("日志内容/异常类型/处理状态已写入索引",
          e1["log_content"] and e1["exception_type"] and e1["process_status"])
    check("事件时间校验通过并保留毫秒时间", e1["time_valid"] and "." in e1["event_time"],
          e1["event_time"])

    # ---------- 中断续做：从 row-206 继续，已归档区间不重读 ----------
    before_files = list(index["completed_files"])
    index2, msg2 = log_archive_service.resume_migration(archive_id)
    check("续做后达到唯一终态「已完成」", index2["state"] == STATE_COMPLETED, msg2)
    e3 = entry_by_record(index2, "LOG-0003")
    check("LOG-0003(row-309) 续做后归档", e3["archived"] and e3["processed_cursor"])
    check("续做位置到 EOF", index2["resume_position"] == "EOF")
    check("已完成文件全部保留且无重复文件",
          sorted(index2["completed_files"]) == sorted(set(index2["completed_files"]))
          and set(before_files).issubset(set(index2["completed_files"])),
          str(index2["completed_files"]))
    check("映射表在完成后冻结，历史归档按原映射兼容", index2["mapping_frozen"] is True)
    check("历史快照链保留（中断+完成至少两个快照）", len(index2["snapshots"]) >= 2,
          f"snapshots={len(index2['snapshots'])}")

    # ---------- 旧行号必须映射到唯一事件时间偏移 ----------
    resolution, rmsg = log_archive_service.resolve_legacy_cursor(archive_id, "row-205")
    check("row-205 唯一映射到事件偏移",
          resolution is not None and resolution["event_offset"] == e2["event_offset"], rmsg)
    bad, bad_msg = log_archive_service.resolve_legacy_cursor(archive_id, "row-999")
    check("无法唯一映射的旧游标被拒绝", bad is None, bad_msg)

    # ---------- 同一归档并发迁移只生效一次，终态唯一 ----------
    barrier = threading.Barrier(5)
    def concurrent_call() -> tuple[str, bool]:
        barrier.wait()
        idx, _msg, ran = log_archive_service.run_migration(archive_id)
        return idx["state"], ran
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        outs = list(pool.map(lambda _: concurrent_call(), range(5)))
    check("并发迁移终态只有「已完成」一个",
          set(state for state, _ in outs) == {STATE_COMPLETED})
    check("并发调用没有产生第二次实际执行",
          all(ran is False for _state, ran in outs))

    # ---------- 另一个执行入口（消费侧）共用同一份索引 ----------
    idx_before_consume = log_archive_service.get_index(archive_id)
    _idx, cmsg, delivered = log_archive_service.consume(archive_id)
    check("消费侧交付全部 3 条已归档记录", len(delivered) == 3, cmsg)
    check("消费侧拿到的游标与索引已处理游标一致",
          {d["processed_cursor"] for d in delivered}
          == {e["processed_cursor"] for e in idx_before_consume["entries"] if e["archived"]})
    idx_after = log_archive_service.get_index(archive_id)
    check("消费动作回写同一份索引（consumed 标记）",
          all(e["consumed"] and e["consumed_cursor"] for e in idx_after["entries"] if e["archived"]))
    _idx2, _m, second = log_archive_service.consume(archive_id)
    check("消费侧不重复交付已处理区间", second == [])

    # ---------- 导出/下载/上传记录/导入核对 ----------
    export_payload, export_msg = log_archive_service.export_index(archive_id)
    check("导出内容含 checksum 与三个游标字段",
          export_payload.get("checksum")
          and all(k in export_payload["entries"][0]
                  for k in ("event_offset", "processed_cursor", "resume_position")),
          export_msg)
    download_payload, dl_msg = log_archive_service.download_index(archive_id)
    latest = log_archive_service.get_index(archive_id)
    directions = [t["direction"] for t in latest["transfers"]]
    check("上传记录与下载结果均已登记",
          "upload" in directions and "download" in directions
          and all(t["result"] == "成功" for t in latest["transfers"]),
          str(directions))

    # 正常导入：逐项核对通过
    imported, imsg = log_archive_service.import_index("LOG-ARCHIVE-COPY", export_payload)
    check("导入核对通过并作为新归档唯一索引生效",
          imported is not None and imported["state"] == STATE_COMPLETED, imsg)

    # 篡改内容后 checksum 必须失配并被拒绝
    tampered = dict(export_payload)
    tampered_entries = [dict(e) for e in tampered["entries"]]
    tampered_entries[0]["log_content"] = "被篡改的日志内容"
    tampered["entries"] = tampered_entries
    rejected, reject_msg = log_archive_service.import_index("LOG-ARCHIVE-TAMPERED", tampered)
    check("内容篡改后导入被 checksum 拒绝", rejected is None, reject_msg)

    # 重复偏移（违反唯一映射）必须被拒绝
    dup = dict(export_payload)
    dup_entries = [dict(e) for e in dup["entries"] if e["time_valid"]]
    dup_entries[1]["event_offset"] = dup_entries[0]["event_offset"]
    dup["entries"] = dup_entries
    # 重算 checksum 以确保失败原因落在唯一性校验而非 checksum
    # （复算口径与导入侧一致：去掉 checksum 及同批生成的最后一条传输记录）
    import hashlib
    import json
    body = {k: v for k, v in dup.items() if k != "checksum"}
    _transfers = body.get("transfers")
    if isinstance(_transfers, list) and _transfers and _transfers[-1].get("checksum") == dup.get("checksum"):
        body = dict(body)
        body["transfers"] = _transfers[:-1]
    dup["checksum"] = hashlib.sha256(
        json.dumps(body, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    rejected2, reject_msg2 = log_archive_service.import_index("LOG-ARCHIVE-DUP", dup)
    check("事件偏移不唯一的索引被拒绝", rejected2 is None, reject_msg2)

    # ---------- 冲突场景：旧行号与新偏移指向不同位置时只信唯一索引 ----------
    conflict_id = "LOG-ARCHIVE-CONFLICT"
    cidx, _m, _ran = log_archive_service.run_migration(conflict_id, stop_after_row=205)
    # 外部旧快照（v1 行号时代）声称 row-205 已处理到另一个位置；终态索引优先
    res, cres_msg = log_archive_service.resolve_legacy_cursor(conflict_id, "row-205")
    check("冲突裁决来源为异常归档数据索引",
          res is not None and res["source"] == "异常归档数据索引" and res["index_state"] == STATE_INTERRUPTED,
          cres_msg)

    # 人为构造真冲突：篡改索引内已处理游标，再用旧行号解析，应裁决为以索引为准
    target = log_archive_service._indexes[conflict_id]  # noqa: SLF001
    original_cursor = target["entries"][1]["processed_cursor"]
    target["entries"][1]["processed_cursor"] = "event-offset=1"
    res2, cmsg2 = log_archive_service.resolve_legacy_cursor(conflict_id, "row-205")
    check("旧行号与新偏移指向不同位置时检测到冲突",
          res2 is not None and res2["conflict"] is True, cmsg2)
    target["entries"][1]["processed_cursor"] = original_cursor
    conflict_records = log_archive_service.get_index(conflict_id)["conflicts"]
    check("冲突裁决只接受唯一索引终态、放弃旧行号来源",
          any(c["decision"].startswith("以异常归档数据索引") for c in conflict_records))

    # ---------- 事件时间异常：校验失败则中断并保留续做位置 ----------
    bad_time_id = "LOG-ARCHIVE-BADTIME"
    bad_row = store.rows("log")[0]
    original_time = bad_row["发生时间"]
    try:
        bad_row["发生时间"] = "not-a-time"
        bidx, bmsg, bran = log_archive_service.run_migration(bad_time_id)
        bad_entry = entry_by_record(bidx, "LOG-0001")
        check("事件时间非法时迁移中断且条目标记异常",
              bidx["state"] == STATE_INTERRUPTED
              and bad_entry["time_valid"] is False
              and bad_entry["archive_status"] == "事件时间异常", bmsg)
        check("异常断点不错位：续做位置停在问题行 row-101",
              bidx["resume_position"] == "row-101", bidx["resume_position"])
        # 修复后续做可以走到终态
        bad_row["发生时间"] = original_time
        ridx, rmsg = log_archive_service.resume_migration(bad_time_id)
        check("修复事件时间后续做达到终态", ridx["state"] == STATE_COMPLETED, rmsg)
    finally:
        bad_row["发生时间"] = original_time

    # ---------- 汇总 ----------
    total = len(results)
    failed = [r for r in results if not r[1]]
    print("\n" + "=" * 60)
    print(f"验收结论：{total - len(failed)}/{total} 通过")
    if failed:
        for name, _ok, detail in failed:
            print(f"  未通过：{name} —— {detail}")
        return 1
    print("全部约束满足：索引保留事件偏移、已处理游标与续做位置。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
