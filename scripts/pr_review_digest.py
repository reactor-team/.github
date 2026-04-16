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
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]
REPOS = os.environ.get("REPOS", "reactor-team/reactor,reactor-team/fluxcd")

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


def get_stale_prs() -> tuple[list[dict], list[dict], int]:
    now = datetime.now(timezone.utc)
    stale, warning, healthy = [], [], 0

    for repo in (r.strip() for r in REPOS.split(",")):
        pulls = gh_api(f"/repos/{repo}/pulls?state=open&per_page=100")

        for pr in pulls:
            if pr.get("draft"):
                continue
            if is_bot(pr.get("user", {})):
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

    stale.sort(key=lambda p: p["staleness"], reverse=True)
    warning.sort(key=lambda p: p["staleness"], reverse=True)
    return stale, warning, healthy


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


def build_slack_message(stale: list, warning: list, healthy: int) -> dict:
    if not stale and not warning:
        return {
            "text": ":clipboard: *PR Review Digest*\n"
                    ":white_check_mark: All clear — no PRs waiting more than 36h for review."
        }

    sections = [":clipboard: *PR Review Digest*\n"]

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
    stale, warning, healthy = get_stale_prs()
    print(f"Found {len(stale)} stale, {len(warning)} warning, {healthy} healthy PRs")

    payload = build_slack_message(stale, warning, healthy)
    print("Posting to Slack:")
    print(payload["text"])
    post_to_slack(payload)
    print("Done.")


if __name__ == "__main__":
    main()
