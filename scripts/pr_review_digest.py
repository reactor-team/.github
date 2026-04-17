#!/usr/bin/env python3
"""PR Review Digest — posts a Slack summary of stale PRs.

Queries GitHub for open PRs across configured repos, buckets them by staleness
(warning >= 36h, stale >= 48h since review was requested or since the last commit
after that request), and posts a formatted digest to a Slack incoming webhook.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Union

GH_TOKEN = os.environ["GH_TOKEN"]
REPOS = os.environ.get("REPOS", "reactor-team/reactor,reactor-team/fluxcd")
DRY_RUN = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "") if DRY_RUN else os.environ["SLACK_WEBHOOK_URL"]

STALE_THRESHOLD = timedelta(hours=48)
WARNING_THRESHOLD = timedelta(hours=36)

BOT_LOGINS = {"dependabot", "renovate", "github-actions"}


def gh_api(path: str) -> Union[list, dict]:
    url = f"https://api.github.com{path}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {GH_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def parse_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def is_bot(user: dict) -> bool:
    login = user.get("login", "")
    return (
        user.get("type") == "Bot"
        or login in BOT_LOGINS
        or login.endswith("[bot]")
    )


def get_merge_state(repo: str, pr_number: int) -> str:
    """Return GitHub's mergeable_state for a PR.

    Possible values: clean, unstable, blocked, behind, dirty, has_hooks, unknown.
    The list endpoint omits this field reliably, so fetch the single-PR endpoint.
    """
    pr = gh_api(f"/repos/{repo}/pulls/{pr_number}")
    return pr.get("mergeable_state", "unknown") if isinstance(pr, dict) else "unknown"


MERGE_STATE_LABELS: dict[str, str] = {
    "clean": ":white_check_mark: ready",
    "has_hooks": ":white_check_mark: ready",
    "unstable": ":white_check_mark: ready",
    "blocked": ":no_entry: blocked",
    "behind": ":arrow_down: behind main",
    "dirty": ":warning: conflicts",
}


def get_approval_status(
    repo: str, pr_number: int
) -> tuple[bool, datetime | None, str | None]:
    """Return (is_approved, latest_approval_time, approver_login).

    Approved iff at least one reviewer's latest non-comment review is APPROVED
    and no reviewer's latest non-comment review is CHANGES_REQUESTED.
    COMMENTED and PENDING reviews are ignored when determining "latest".
    """
    reviews = gh_api(f"/repos/{repo}/pulls/{pr_number}/reviews?per_page=100")

    latest_per_user: dict[str, tuple[datetime, str]] = {}
    for r in reviews:
        state = r.get("state")
        if state in ("COMMENTED", "PENDING"):
            continue
        user = (r.get("user") or {}).get("login")
        submitted_at = r.get("submitted_at")
        if not user or not submitted_at:
            continue
        submitted = parse_dt(submitted_at)
        if user not in latest_per_user or submitted > latest_per_user[user][0]:
            latest_per_user[user] = (submitted, state)

    if any(state == "CHANGES_REQUESTED" for _, state in latest_per_user.values()):
        return (False, None, None)

    approvals = [
        (t, user) for user, (t, state) in latest_per_user.items() if state == "APPROVED"
    ]
    if not approvals:
        return (False, None, None)

    latest_time, latest_user = max(approvals)
    return (True, latest_time, latest_user)


def get_clock_start(repo: str, pr_number: int, pr_created_at: str) -> datetime:
    """Return max(earliest review_requested event, last commit after that event)."""
    timeline = gh_api(f"/repos/{repo}/issues/{pr_number}/timeline?per_page=100")
    review_events = [
        e for e in timeline
        if e.get("event") == "review_requested" and e.get("created_at")
    ]

    if not review_events:
        return parse_dt(pr_created_at)

    earliest_request = min(parse_dt(e["created_at"]) for e in review_events)

    commits = gh_api(f"/repos/{repo}/pulls/{pr_number}/commits?per_page=100")
    if commits:
        last_commit_date = parse_dt(commits[-1]["commit"]["committer"]["date"])
        if last_commit_date > earliest_request:
            return last_commit_date

    return earliest_request


def get_stale_prs() -> tuple[list[dict], list[dict], list[dict], int]:
    now = datetime.now(timezone.utc)
    approved, stale, warning, healthy = [], [], [], 0

    for repo in (r.strip() for r in REPOS.split(",")):
        pulls = gh_api(f"/repos/{repo}/pulls?state=open&per_page=100")

        for pr in pulls:
            if pr.get("draft"):
                continue
            if is_bot(pr.get("user", {})):
                continue

            is_approved, approved_at, approver = get_approval_status(repo, pr["number"])

            if is_approved:
                merge_state = get_merge_state(repo, pr["number"])
                approved.append({
                    "repo": repo.split("/")[-1],
                    "number": pr["number"],
                    "title": pr["title"],
                    "url": pr["html_url"],
                    "author": pr["user"]["login"],
                    "approver": approver,
                    "approved_age": now - approved_at,
                    "merge_state": merge_state,
                })
                continue

            reviewers = [r["login"] for r in pr.get("requested_reviewers", [])]
            if not reviewers:
                continue

            clock_start = get_clock_start(repo, pr["number"], pr["created_at"])
            staleness = now - clock_start

            entry = {
                "repo": repo.split("/")[-1],
                "number": pr["number"],
                "title": pr["title"],
                "url": pr["html_url"],
                "author": pr["user"]["login"],
                "reviewers": reviewers,
                "staleness": staleness,
            }

            if staleness >= STALE_THRESHOLD:
                stale.append(entry)
            elif staleness >= WARNING_THRESHOLD:
                warning.append(entry)
            else:
                healthy += 1

    approved.sort(key=lambda p: p["approved_age"], reverse=True)
    stale.sort(key=lambda p: p["staleness"], reverse=True)
    warning.sort(key=lambda p: p["staleness"], reverse=True)
    return approved, stale, warning, healthy


def format_staleness(td: timedelta) -> str:
    total_hours = int(td.total_seconds() // 3600)
    if total_hours >= 48:
        days, hours = divmod(total_hours, 24)
        return f"{days}d {hours}h" if hours else f"{days}d"
    return f"{total_hours}h"


def format_pr(pr: dict) -> str:
    reviewers = ", ".join(f"@{r}" for r in pr["reviewers"])
    staleness = format_staleness(pr["staleness"])
    return (
        f"• *{pr['repo']}* <{pr['url']}|#{pr['number']}> — {pr['title']} ({staleness})\n"
        f"  Reviewers: {reviewers}"
    )


def format_approved_pr(pr: dict) -> str:
    age = format_staleness(pr["approved_age"])
    tag = MERGE_STATE_LABELS.get(pr["merge_state"], "")
    tag_suffix = f" · {tag}" if tag else ""
    return (
        f"• *{pr['repo']}* <{pr['url']}|#{pr['number']}> — {pr['title']}{tag_suffix}\n"
        f"  Approved {age} ago by @{pr['approver']} · author @{pr['author']}"
    )


def build_slack_message(
    approved: list, stale: list, warning: list, healthy: int
) -> dict:
    if not approved and not stale and not warning:
        return {
            "text": ":clipboard: *PR Review Digest*\n"
                    ":white_check_mark: All clear — no PRs waiting more than 36h for review."
        }

    sections = [":clipboard: *PR Review Digest*\n"]

    if approved:
        sections.append(":white_check_mark: *Approved — ready to merge*")
        sections.extend(format_approved_pr(pr) for pr in approved)
        sections.append("")

    if stale:
        sections.append(":rotating_light: *Stale (48h+)*")
        sections.extend(format_pr(pr) for pr in stale)
        sections.append("")

    if warning:
        sections.append(":eyes: *Needs attention (36h+)*")
        sections.extend(format_pr(pr) for pr in warning)
        sections.append("")

    if healthy:
        plural = "s" if healthy != 1 else ""
        verb = "are" if healthy != 1 else "is"
        sections.append(f"_{healthy} other open PR{plural} {verb} healthy (<36h)_")

    return {"text": "\n".join(sections)}


def post_to_slack(payload: dict) -> None:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        if resp.status != 200:
            print(f"Slack webhook returned {resp.status}: {resp.read()}", file=sys.stderr)
            sys.exit(1)


def main() -> None:
    print(f"Fetching open PRs from: {REPOS}")
    approved, stale, warning, healthy = get_stale_prs()
    print(
        f"Found {len(approved)} approved, {len(stale)} stale, "
        f"{len(warning)} warning, {healthy} healthy PRs"
    )

    payload = build_slack_message(approved, stale, warning, healthy)
    if DRY_RUN:
        print("[DRY_RUN] Would post to Slack:")
        print(payload["text"])
        return

    print("Posting to Slack:")
    print(payload["text"])
    post_to_slack(payload)
    print("Done.")


if __name__ == "__main__":
    main()
