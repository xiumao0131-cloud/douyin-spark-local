"""命令行入口。"""

from __future__ import annotations

import argparse
import asyncio
import json

import uvicorn

from spark import config, log, messages, state


def _status():
    day = state.today()
    stats = state.day_stats(day)
    check = state.latest_session_check()
    friends = state.list_friends()
    last_run = state.latest_run_today()
    payload = {
        "day": day,
        "stats": stats,
        "login": {
            "day": check["day"] if check else None,
            "ok": bool(check["ok"]) if check else None,
            "note": check["note"] if check else "",
        },
        "friends": len(friends),
        "enabledFriends": sum(1 for row in friends if row["enabled"]),
        "poolSize": len(messages.load_pool()),
        "paused": config.load_config().get("risk", {}).get("paused", False),
        "lastRun": {
            "trigger": last_run["trigger"] if last_run else None,
            "status": last_run["status"] if last_run else None,
        },
    }
    return payload


def main():
    parser = argparse.ArgumentParser(prog="spark", description="抖音续火花本地版")
    parser.add_argument("command", nargs="?", default="serve")
    parser.add_argument("--json", action="store_true", help="status 输出 JSON")
    parser.add_argument("--catch-up", action="store_true", help="仅在窗口内且今天未跑过时才发送")
    args = parser.parse_args()

    log.configure_logging()
    state.ensure_schema()

    if args.command == "serve":
        cfg = config.load_config()
        port = int(cfg.get("ui", {}).get("port", 8899))
        uvicorn.run("spark.server:app", host="127.0.0.1", port=port, log_level="warning")
        return

    from spark import service

    if args.command == "login":
        asyncio.run(service.login_flow())
        return
    if args.command == "run":
        if args.catch_up:
            from datetime import datetime

            from spark import scheduler

            cfg = config.load_config()
            decision = scheduler.decide_now(
                datetime.now(scheduler.TZ),
                cfg["schedule"]["windowStart"],
                cfg["schedule"]["windowEnd"],
                ran_today=state.latest_run_today() is not None,
            )
            if decision != "run":
                print(f"不满足补跑条件（{decision}）")
                return
        asyncio.run(service.run_round(trigger="catchup" if args.catch_up else "manual"))
        return
    if args.command == "status":
        payload = _status()
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(f"日期: {payload['day']}  登录: {payload['login']['ok']}  发送已确认: "
                  f"{payload['stats']['confirmed']}  好友: {payload['enabledFriends']}/{payload['friends']}  "
                  f"文案池: {payload['poolSize']}  暂停: {payload['paused']}  "
                  f"最近一轮: {payload['lastRun']['trigger']}/{payload['lastRun']['status']}")
        return
    if args.command == "pause":
        cfg = config.load_config()
        cfg["risk"]["paused"] = True
        config.save_config(cfg)
        print("已暂停")
        return
    if args.command == "resume":
        cfg = config.load_config()
        cfg["risk"]["paused"] = False
        config.save_config(cfg)
        print("已恢复")
        return
    parser.error(f"未知命令：{args.command}")


if __name__ == "__main__":
    main()
