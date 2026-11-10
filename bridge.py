#!/usr/bin/env python3
"""SHB Web Bridge - 通用 shb-cli 调用器（命令白名单由 app.py 控制）

用法: bridge.py <shb-cli 参数...>
示例: bridge.py paas form search --data '{"templateBizId":"..."}' -o raw
"""
import json
import os
import subprocess
import sys

SHB_BIN = "shb-cli"
TIMEOUT = 60


def run_cli(argv: list, timeout: int = TIMEOUT) -> dict:
    """调用 shb-cli（走 SHB_WEB_GUARD 白名单通道）"""
    try:
        env = {**os.environ, "NO_COLOR": "1", "SHB_WEB_GUARD": "1"}
        proc = subprocess.run(
            [SHB_BIN] + argv,
            capture_output=True, text=True, timeout=timeout, env=env,
        )
        return {"ok": proc.returncode == 0, "code": proc.returncode,
                "stdout": proc.stdout, "stderr": proc.stderr}
    except subprocess.TimeoutExpired:
        return {"ok": False, "code": -1, "stdout": "", "stderr": "shb-cli 调用超时"}
    except FileNotFoundError:
        return {"ok": False, "code": -1, "stdout": "", "stderr": "shb-cli 不存在"}
    except Exception as e:
        return {"ok": False, "code": -1, "stdout": "", "stderr": str(e)}


def parse_json_output(stdout: str):
    """解析 shb-cli JSON 输出（容忍多余前缀行）"""
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith(("{", "[")):
            try:
                return json.loads(line)
            except Exception:
                continue
    return None


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"ok": False, "error": "缺少命令参数"}, ensure_ascii=False))
        sys.exit(1)

    r = run_cli(sys.argv[1:])
    if not r["ok"]:
        print(json.dumps({"ok": False, "error": (r["stderr"] or r["stdout"])[:500]}, ensure_ascii=False))
        sys.exit(0)

    # 输出原始 stdout（JSON 或文本）
    sys.stdout.write(r["stdout"])
