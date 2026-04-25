import atexit
import json
import os
import subprocess
import time
import urllib.error
import urllib.request

import runpod


RUST_BINARY = os.environ.get("SERVERLESS_DB_BINARY", "/usr/local/bin/serverless-db")
INTERNAL_HOST = os.environ.get("SERVERLESS_DB_INTERNAL_HOST", "127.0.0.1")
INTERNAL_PORT = int(os.environ.get("SERVERLESS_DB_INTERNAL_PORT", "8080"))
INTERNAL_BASE_URL = f"http://{INTERNAL_HOST}:{INTERNAL_PORT}"

_server_process = None


def _start_server():
    global _server_process
    if _server_process is not None and _server_process.poll() is None:
        return

    env = os.environ.copy()
    env["SERVERLESS_DB_BIND"] = f"{INTERNAL_HOST}:{INTERNAL_PORT}"

    _server_process = subprocess.Popen(
        [RUST_BINARY],
        env=env,
    )

    deadline = time.time() + 20
    while time.time() < deadline:
        if _server_process.poll() is not None:
            raise RuntimeError("serverless-db process exited during startup")

        try:
            with urllib.request.urlopen(f"{INTERNAL_BASE_URL}/health", timeout=1) as response:
                if response.status == 200:
                    return
        except Exception:
            time.sleep(0.25)

    raise RuntimeError("serverless-db failed to become healthy in time")


def _stop_server():
    global _server_process
    if _server_process is None:
        return
    if _server_process.poll() is None:
        _server_process.terminate()
        try:
            _server_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _server_process.kill()
            _server_process.wait(timeout=5)
    _server_process = None


atexit.register(_stop_server)


def _post_json(path, payload):
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{INTERNAL_BASE_URL}{path}",
        data=data,
        headers={"content-type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8")
        try:
            return json.loads(body)
        except json.JSONDecodeError as decode_error:
            raise RuntimeError(f"serverless-db returned invalid JSON: {decode_error}") from error


def handler(job):
    _start_server()

    job_input = job.get("input") or {}
    sql = job_input.get("sql")
    database = job_input.get("database")

    if not isinstance(sql, str) or not sql.strip():
        return {
            "ok": False,
            "status": 400,
            "error": "input.sql must be a non-empty string",
        }

    payload = {"sql": sql, "database": database}
    return _post_json("/sql", payload)


if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})

