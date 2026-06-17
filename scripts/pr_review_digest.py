#!/usr/bin/env python3
"""PR Review Digest — posts a Slack summary of stale PRs.

Queries GitHub for open PRs across configured repos and reports those that have
been waiting >= 48h for review (since review was requested or since the last
commit after that request), then posts a formatted digest to a Slack incoming
webhook. Approved PRs are excluded — they no longer need review.
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


def get_stale_prs() -> tuple[list[dict], int]:
    """Return (stale_prs, under_threshold_count).

    Stale = open, non-draft, non-bot, not approved, has requested reviewers, and
    waiting >= STALE_THRESHOLD since review was requested / last commit after it.
    """
    now = datetime.now(timezone.utc)
    stale, under_threshold = [], 0

    for repo in (r.strip() for r in REPOS.split(",")):
        pulls = gh_api(f"/repos/{repo}/pulls?state=open&per_page=100")

        for pr in pulls:
            if pr.get("draft"):
                continue
            if is_bot(pr.get("user", {})):
                continue

            # Approved PRs no longer need review — exclude them entirely.
            is_approved, _, _ = get_approval_status(repo, pr["number"])
            if is_approved:
                continue

            reviewers = [r["login"] for r in pr.get("requested_reviewers", [])]
            if not reviewers:
                continue

            clock_start = get_clock_start(repo, pr["number"], pr["created_at"])
            staleness = now - clock_start

            if staleness < STALE_THRESHOLD:
                under_threshold += 1
                continue

            stale.append({
                "repo": repo.split("/")[-1],
                "number": pr["number"],
                "title": pr["title"],
                "url": pr["html_url"],
                "author": pr["user"]["login"],
                "reviewers": reviewers,
                "staleness": staleness,
            })

    stale.sort(key=lambda p: p["staleness"], reverse=True)
    return stale, under_threshold


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


def build_slack_message(stale: list, under_threshold: int) -> dict:
    if not stale:
        return {
            "text": ":clipboard: *PR Review Digest*\n"
                    ":white_check_mark: All clear — no PRs waiting more than 48h for review."
        }

    sections = [":clipboard: *PR Review Digest*\n"]
    sections.append(":rotating_light: *Stale (48h+)*")
    sections.extend(format_pr(pr) for pr in stale)

    if under_threshold:
        plural = "s" if under_threshold != 1 else ""
        verb = "are" if under_threshold != 1 else "is"
        sections.append("")
        sections.append(f"_{under_threshold} other open PR{plural} {verb} under 48h_")

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
    stale, under_threshold = get_stale_prs()
    print(f"Found {len(stale)} stale (48h+), {under_threshold} under 48h")

    payload = build_slack_message(stale, under_threshold)
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
