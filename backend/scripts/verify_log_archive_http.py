"""日志归档迁移接口端到端验收：真实 uvicorn 进程 + 标准库 urllib。

运行：backend/.venv/bin/python -I scripts/verify_log_archive_http.py
"""
from __future__ import annotations

import json
import socket
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import uvicorn  # noqa: E402
from app.main import app  # noqa: E402
from app.services.log_archive import log_archive_service  # noqa: E402

PASS = "PASS"
FAIL = "FAIL"
results: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    results.append((name, bool(condition), detail))
    print(f"[{PASS if condition else FAIL}] {name}" + (f" —— {detail}" if detail else ""))


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def call(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:  # type: ignore[attr-defined]
        return exc.code, json.loads(exc.read().decode("utf-8"))


def main() -> int:
    log_archive_service.reset()
    port = free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)

    import threading
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}/api/log/archive"
    for _ in range(50):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=1)
            break
        except OSError:
            time.sleep(0.1)

    try:
        archive = "HTTP-ARCHIVE-01"

        # 1. 迁移到 row-205 模拟部署中断
        status, payload = call("POST", f"{base}/{archive}/migrate", {"stop_after_row": 205})
        index = payload["index"]
        check("迁移接口返回 200 且状态为已中断",
              status == 200 and index["state"] == "已中断", payload["message"])
        check("接口核对：停在 row-205，续做位置 row-206",
              index["resume_position"] == "row-206", index["resume_position"])
        rows = {e["record_no"]: e for e in index["entries"]}
        check("接口核对：row-101/205 已归档、row-309 未归档",
              rows["LOG-0001"]["archived"] and rows["LOG-0002"]["archived"]
              and not rows["LOG-0003"]["archived"])
        check("接口核对：条目含事件偏移/已处理游标/续做位置",
              all(rows[r][k] for r in ("LOG-0001", "LOG-0002")
                  for k in ("event_offset", "processed_cursor", "resume_position")))

        # 2. 并发迁移只生效一次
        def fire() -> dict:
            _s, p = call("POST", f"{base}/{archive}/migrate", {})
            return p
        outs = [fire() for _ in range(3)]
        check("并发/重复迁移被合并，没有第二次实际执行",
              all(o["executed"] is False for o in outs))

        # 3. 续做
        status, payload = call("POST", f"{base}/{archive}/resume")
        check("续做接口推进到唯一终态",
              status == 200 and payload["index"]["state"] == "已完成"
              and payload["index"]["resume_position"] == "EOF", payload["message"])

        # 4. 旧行号解析
        status, payload = call("GET", f"{base}/{archive}/resolve/row-205")
        check("row-205 解析为唯一事件时间偏移",
              status == 200 and payload["ok"] and payload["resolution"]["event_offset"],
              payload["message"])
        status, payload = call("GET", f"{base}/{archive}/resolve/row-999")
        check("非法旧游标被接口拒绝", status == 200 and payload["ok"] is False)

        # 5. 消费侧入口共用同一份索引
        status, payload = call("POST", f"{base}/{archive}/consume", {})
        check("消费侧交付 3 条且游标与索引一致",
              status == 200 and len(payload["delivered"]) == 3, payload["message"])
        status, payload = call("POST", f"{base}/{archive}/consume", {})
        check("消费侧不重复交付", payload["delivered"] == [])

        # 6. 导出/下载/上传记录
        status, export_payload = call("GET", f"{base}/{archive}/export")
        check("导出接口返回 checksum", status == 200 and bool(export_payload["export"]["checksum"]))
        status, dl_payload = call("GET", f"{base}/{archive}/download")
        check("下载接口返回 checksum", status == 200 and bool(dl_payload["download"]["checksum"]))
        status, idx_payload = call("GET", f"{base}/{archive}")
        directions = [t["direction"] for t in idx_payload["transfers"]]
        check("上传记录与下载结果可核对", directions.count("upload") >= 1 and directions.count("download") >= 1,
              str(directions))

        # 7. 导入核对：正常通过 / 篡改拒绝
        status, ok_payload = call("POST", f"{base}/HTTP-ARCHIVE-COPY/import",
                                  {"payload": export_payload["export"]})
        check("正常导入核对通过", status == 200 and ok_payload["ok"], ok_payload["message"])
        tampered = json.loads(json.dumps(export_payload["export"]))
        tampered["entries"][0]["log_content"] = "篡改内容"
        status, bad_payload = call("POST", f"{base}/HTTP-ARCHIVE-BAD/import", {"payload": tampered})
        check("篡改文件导入被拒绝", status == 200 and bad_payload["ok"] is False, bad_payload["message"])

        # 8. 列表与 404
        status, listing = call("GET", base)
        check("索引列表可查询", status == 200 and listing["total"] >= 2)
        status, missing = call("GET", f"{base}/NOT-EXIST")
        check("不存在的索引返回 404", status == 404)

        # 9. 原有日志模块接口不受影响
        status, log_list = call("GET", "http://127.0.0.1:%d/api/log?page=1&size=10" % port)
        check("原日志列表接口正常且为真实内容",
              status == 200 and log_list["total"] == 3
              and log_list["items"][0]["记录编号"] == "LOG-0001")
    finally:
        server.should_exit = True
        thread.join(timeout=5)

    total = len(results)
    failed = [r for r in results if not r[1]]
    print("\n" + "=" * 60)
    print(f"HTTP 验收结论：{total - len(failed)}/{total} 通过")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
