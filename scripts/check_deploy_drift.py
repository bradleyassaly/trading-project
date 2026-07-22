"""Deploy-drift guard: alert when the running system diverges from origin/main.

Root cause this kills (2026-07-16 eval): docker-compose bind-mounts .:/app,
so "deployed" requires BOTH (a) local main == origin/main and (b) every
long-lived python process restarted AFTER the last code change. Both halves
have silently failed for days at a time: the EV-guard/scheduler-recovery
fixes sat merged on origin while prod ran the older local tree, and earlier
fixes sat pulled-but-unimported because nobody bounced the container. No
check owned this, so "the fix is merged" kept meaning nothing.

Two checks, both filesystem-only for local state (the image has no git):
  1. PULL drift    — local branch sha (read straight from /app/.git plumbing:
     HEAD -> ref file -> packed-refs) vs origin's sha for the same branch via
     the GitHub API (repo is public; set GITHUB_TOKEN if that ever changes).
     Any mismatch alerts: behind = merged fixes not running, ahead/unknown =
     un-pushed local commits (the other half of the same disease).
  2. RESTART drift — this container's PID-1 start time (/proc/1/stat
     starttime vs /proc/uptime) against the last local ref movement
     (.git/logs/HEAD mtime). Code moved after the process started means the
     scheduler is executing stale imports. PID 1 here IS task_scheduler.py,
     because this script runs as one of its subprocess tasks — which is also
     WHY it's a subprocess task and not a watchdog method: a subprocess loads
     fresh code every run, so the guard cannot itself rot with the thing it
     guards.

Exit codes: 0 clean, 3 drift (Telegram alert attempted), 1 check error —
nonzero on purpose so scheduler consecutive-failure tracking is a second
alert path if Telegram is down.

Run: python scripts/check_deploy_drift.py [--verbose]
Scheduled: task 'deploy_drift' (6h).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.request

GIT_DIR = os.environ.get("DEPLOY_DRIFT_GIT_DIR", "/app/.git")
BRANCH = os.environ.get("DEPLOY_DRIFT_BRANCH", "main")
# A pull is normally followed by a container bounce within minutes; don't
# page inside that window.
RESTART_GRACE_S = int(os.environ.get("DEPLOY_DRIFT_RESTART_GRACE_S", str(30 * 60)))
API_TIMEOUT_S = 20


def _read(path: str) -> str:
    with open(path, encoding="utf-8", errors="replace") as fh:
        return fh.read()


def checked_out_ref(git_dir: str = GIT_DIR) -> str | None:
    """The ref HEAD points at, e.g. 'refs/heads/main'; None if detached."""
    head = _read(os.path.join(git_dir, "HEAD")).strip()
    return head[5:].strip() if head.startswith("ref: ") else None


def resolve_ref(ref: str, git_dir: str = GIT_DIR) -> str:
    """Resolve a full ref name to a sha via loose ref file or packed-refs."""
    ref_path = os.path.join(git_dir, *ref.split("/"))
    if os.path.exists(ref_path):
        return _read(ref_path).strip()
    packed = os.path.join(git_dir, "packed-refs")
    if os.path.exists(packed):
        for line in _read(packed).splitlines():
            line = line.strip()
            if line and not line.startswith(("#", "^")) and line.endswith(" " + ref):
                return line.split(" ", 1)[0]
    raise RuntimeError(f"cannot resolve {ref} under {git_dir}")


def github_owner_repo(git_dir: str = GIT_DIR) -> tuple[str, str]:
    cfg = _read(os.path.join(git_dir, "config"))
    m = re.search(r'\[remote "origin"\][^\[]*?url\s*=\s*(\S+)', cfg)
    if not m:
        raise RuntimeError("no [remote origin] url in .git/config")
    m2 = re.search(r"github\.com[:/]([^/\s]+)/([^/\s]+?)(?:\.git)?$", m.group(1))
    if not m2:
        raise RuntimeError(f"origin url not a github repo: {m.group(1)}")
    return m2.group(1), m2.group(2)


def _gh_get(path: str) -> dict:
    req = urllib.request.Request(
        f"https://api.github.com{path}",
        headers={"Accept": "application/vnd.github+json",
                 "User-Agent": "trading-platform-deploy-drift"})
    tok = os.environ.get("GITHUB_TOKEN", "").strip()
    if tok:
        req.add_header("Authorization", f"Bearer {tok}")
    with urllib.request.urlopen(req, timeout=API_TIMEOUT_S) as resp:
        return json.load(resp)


def origin_sha(owner: str, repo: str, branch: str = BRANCH) -> str:
    return _gh_get(f"/repos/{owner}/{repo}/commits/{branch}")["sha"]


def relation(owner: str, repo: str, local: str, branch: str = BRANCH) -> str:
    """How origin/<branch> stands relative to the local sha (best-effort)."""
    try:
        status = _gh_get(f"/repos/{owner}/{repo}/compare/{local}...{branch}")["status"]
        return {"ahead": "origin is AHEAD (merged fixes are NOT running here)",
                "behind": "local is AHEAD (un-pushed commits)",
                "diverged": "histories DIVERGED",
                "identical": "identical"}.get(status, status)
    except Exception:
        # 404 = origin has never seen the local sha: un-pushed or rewritten.
        return "local sha unknown to origin (un-pushed commits or rewritten history)"


def pid1_started_at() -> float | None:
    """Epoch seconds PID 1 (this container's service process) started."""
    try:
        stat = _read("/proc/1/stat")
        fields_after_comm = stat.rsplit(")", 1)[1].split()
        start_jiffies = int(fields_after_comm[19])  # overall field 22: starttime
        uptime_s = float(_read("/proc/uptime").split()[0])
        started_ago = uptime_s - start_jiffies / os.sysconf("SC_CLK_TCK")
        return time.time() - started_ago
    except Exception:
        return None  # not linux / no procfs — skip the restart check


def code_moved_at(git_dir: str = GIT_DIR) -> float | None:
    """Epoch seconds of the last local ref movement (commit/pull/rebase)."""
    stamps = []
    for rel in ("logs/HEAD", f"refs/heads/{BRANCH}", "packed-refs"):
        p = os.path.join(git_dir, rel)
        if os.path.exists(p):
            stamps.append(os.path.getmtime(p))
    return max(stamps) if stamps else None


def lint_env_file(path: str = ".env") -> list[str]:
    """Catch .env corruption before it silently disarms flags on a recreate.

    2026-07-21 incident: `echo >>` onto a last-line-without-newline produced
    'MAKER_EXPERIMENT_LIVE=1WS_FORCE_BROAD=1' as ONE line — both flags invalid
    on any container recreate (the live maker experiment would have silently
    disarmed). Rules: every non-comment line matches KEY=VALUE with a sane
    key; no duplicate keys; no key containing '=' remnants of a mangled join
    (a VALUE containing 'X_Y=' where X_Y looks like an env key).
    """
    import re
    problems: list[str] = []
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            lines = fh.read().splitlines()
    except FileNotFoundError:
        return [f".env missing at {path}"]
    seen: dict[str, int] = {}
    key_rx = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    mangle_rx = re.compile(r"[A-Z0-9][A-Z0-9_]{3,}=")
    for i, ln in enumerate(lines, 1):
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        if "=" not in s:
            problems.append(f".env:{i} not KEY=VALUE: {s[:60]!r}")
            continue
        key, val = s.split("=", 1)
        if not key_rx.match(key):
            problems.append(f".env:{i} malformed key {key[:40]!r}")
        if key in seen:
            problems.append(f".env:{i} duplicate key {key} (also line {seen[key]})")
        seen[key] = i
        # A value that itself contains SOMETHING_LIKE_A_KEY= is the mangled-
        # join signature (unless quoted, comma-listed, or a URL query).
        if (mangle_rx.search(val) and not val.startswith(('"', "'"))
                and "://" not in val and "," not in val):
            problems.append(
                f".env:{i} value of {key} looks like a mangled join: {val[:60]!r}")
    return problems


def _alert(message: str) -> None:
    try:
        from trading_platform.polymarket.telegram_alerts import get_alerter
        a = get_alerter()
        if a.enabled:
            a.send_pipeline_alert(component="deploy_drift", message=message,
                                  level="critical")
    except Exception as exc:  # alert failure must not mask the exit code
        print(f"[deploy-drift] alert send failed: {exc}", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    problems: list[str] = []
    problems.extend(lint_env_file())

    ref = checked_out_ref()
    expected_ref = f"refs/heads/{BRANCH}"
    if ref is None:
        problems.append(f"prod tree has a DETACHED HEAD, not {expected_ref}")
    elif ref != expected_ref:
        problems.append(f"prod tree is checked out on {ref}, not {expected_ref}")
    local = resolve_ref(expected_ref)

    owner, repo = github_owner_repo()
    try:
        remote = origin_sha(owner, repo)
    except Exception as exc:
        # Network flake: no drift verdict possible. Exit 1 so the scheduler's
        # consecutive-failure tracking notices if the guard goes blind.
        print(f"[deploy-drift] origin lookup failed: {exc}", file=sys.stderr)
        return 1

    if remote != local:
        rel = relation(owner, repo, local)
        problems.append(
            f"PULL drift on {BRANCH}: local {local[:9]} vs origin {remote[:9]} "
            f"— {rel}. Deploy = git pull --ff-only + restart the python "
            f"containers (OPERATIONS.md 'Deploying code changes').")

    started = pid1_started_at()
    moved = code_moved_at()
    if started and moved and moved > started and time.time() - moved > RESTART_GRACE_S:
        problems.append(
            f"RESTART drift: code last moved {(time.time() - moved) / 3600:.1f}h "
            f"ago but this container's service started "
            f"{(time.time() - started) / 3600:.1f}h ago — long-lived processes "
            f"are running STALE imports. Restart the python containers.")

    if args.verbose or problems:
        age_h = f"{(time.time() - started) / 3600:.1f}" if started else "n/a"
        print(f"[deploy-drift] local={local[:9]} origin={remote[:9]} "
              f"ref={ref} pid1_age_h={age_h}")

    if not problems:
        print(f"[deploy-drift] OK: {BRANCH} in sync ({local[:9]}) and no "
              f"restart drift")
        return 0

    msg = "DEPLOY DRIFT: " + " | ".join(problems)
    print(f"[deploy-drift] {msg}")
    _alert(msg)
    return 3


if __name__ == "__main__":
    sys.exit(main())
