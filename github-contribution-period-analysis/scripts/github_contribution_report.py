#!/usr/bin/env python3
"""Generate a Markdown report of GitHub code contributions in a time window."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_BASE_URL = "https://api.github.com"
DEFAULT_ACCEPT = "application/vnd.github+json"
COMMIT_SEARCH_ACCEPT = "application/vnd.github.cloak-preview+json"
API_VERSION = "2022-11-28"


@dataclass
class SearchMeta:
    query: str
    total_count: int
    fetched: int
    incomplete_results: bool
    truncated_by_cap: bool
    cap: int


@dataclass
class PRRecord:
    key: str
    repo_full_name: str
    number: int
    title: str
    html_url: str
    author_login: str
    merged_by_login: str
    merged_at: str
    additions: int
    deletions: int
    changed_files: int
    commits: int
    merge_commit_sha: str


@dataclass
class CommitRecord:
    sha: str
    repo_full_name: str
    html_url: str
    api_url: str
    message_title: str
    authored_at: str
    additions: Optional[int] = None
    deletions: Optional[int] = None
    changed_files: Optional[int] = None
    file_paths: Optional[List[str]] = None


@dataclass
class WorkItem:
    date_utc: str
    kind: str
    category: str
    repo_full_name: str
    evidence_text: str
    evidence_url: str
    content: str
    additions: Optional[int]
    deletions: Optional[int]
    changed_files: Optional[int]
    file_paths: Optional[List[str]] = None


class GitHubClient:
    def __init__(self, token: str, request_pause: float = 0.0) -> None:
        self.token = token.strip()
        self.request_pause = max(0.0, request_pause)
        self.last_headers: Dict[str, str] = {}

    def request_json(
        self,
        path_or_url: str,
        params: Optional[Dict[str, Any]] = None,
        accept: str = DEFAULT_ACCEPT,
    ) -> Tuple[Any, Dict[str, str]]:
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            url = path_or_url
        else:
            url = f"{API_BASE_URL}{path_or_url}"

        if params:
            query = urlencode(params)
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}{query}"

        headers = {
            "Accept": accept,
            "User-Agent": "github-contribution-period-analysis",
            "X-GitHub-Api-Version": API_VERSION,
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        request = Request(url=url, headers=headers, method="GET")

        try:
            with urlopen(request, timeout=60) as response:
                raw = response.read().decode("utf-8")
                data: Any = json.loads(raw) if raw else {}
                response_headers = {k.lower(): v for k, v in response.headers.items()}
                self.last_headers = response_headers
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            message = _extract_error_message(body)
            raise RuntimeError(f"GitHub API error {exc.code} on {url}: {message}") from exc
        except URLError as exc:
            raise RuntimeError(f"Network error on {url}: {exc.reason}") from exc

        if self.request_pause > 0:
            time.sleep(self.request_pause)

        return data, response_headers

    def search(
        self,
        endpoint: str,
        query: str,
        *,
        max_items: int,
        accept: str,
        sort: Optional[str] = None,
        order: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], SearchMeta]:
        capped = max(0, min(max_items, 1000))
        per_page = 100
        page = 1
        items: List[Dict[str, Any]] = []
        total_count = 0
        incomplete_results = False

        while len(items) < capped:
            params: Dict[str, Any] = {
                "q": query,
                "per_page": per_page,
                "page": page,
            }
            if sort:
                params["sort"] = sort
            if order:
                params["order"] = order

            payload, _ = self.request_json(endpoint, params=params, accept=accept)

            if page == 1:
                total_count = int(payload.get("total_count", 0) or 0)
                incomplete_results = bool(payload.get("incomplete_results", False))

            page_items = payload.get("items") or []
            if not isinstance(page_items, list) or not page_items:
                break

            remaining = capped - len(items)
            items.extend(page_items[:remaining])

            if len(page_items) < per_page:
                break

            page += 1
            if page > 10:
                break

        meta = SearchMeta(
            query=query,
            total_count=total_count,
            fetched=len(items),
            incomplete_results=incomplete_results,
            truncated_by_cap=total_count > len(items),
            cap=capped,
        )
        return items, meta


def _extract_error_message(body: str) -> str:
    if not body:
        return "empty response body"
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return body.strip()[:400]

    if isinstance(payload, dict):
        message = payload.get("message")
        errors = payload.get("errors")
        if isinstance(message, str) and errors:
            return f"{message}; errors={errors}"
        if isinstance(message, str):
            return message
    return str(payload)[:400]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze a user's merged PR and commit contributions in a UTC time window "
            "and emit a Markdown report."
        )
    )
    parser.add_argument("--user", required=True, help="GitHub login to analyze, for example octocat")
    parser.add_argument("--start", required=True, help="UTC start boundary, YYYY-MM-DD or ISO datetime")
    parser.add_argument("--end", required=True, help="UTC end boundary (exclusive), YYYY-MM-DD or ISO datetime")
    parser.add_argument(
        "--output",
        default="github-contribution-report.md",
        help="Output Markdown path, or '-' to print to stdout",
    )
    parser.add_argument("--token", default="", help="GitHub token (defaults to env or gh auth token)")
    parser.add_argument("--max-prs", type=int, default=500, help="Max merged PRs to fetch per query (<=1000)")
    parser.add_argument("--max-commits", type=int, default=1000, help="Max commits to fetch (<=1000)")
    parser.add_argument(
        "--max-commit-detail",
        type=int,
        default=300,
        help="Max direct commits for which to fetch per-commit stats",
    )
    parser.add_argument(
        "--max-pr-commit-pages",
        type=int,
        default=20,
        help="Max pages (100 commits/page) when loading PR commit SHAs for dedupe",
    )
    parser.add_argument(
        "--max-table-rows",
        type=int,
        default=30,
        help="Max rows per detail table in Markdown output",
    )
    parser.add_argument(
        "--max-repo-detail-rows",
        type=int,
        default=8,
        help="Max concrete change rows per repository in repository detail section",
    )
    parser.add_argument(
        "--no-pr-commit-dedupe",
        action="store_true",
        help="Do not remove commits that are already part of authored merged PRs",
    )
    parser.add_argument(
        "--request-pause",
        type=float,
        default=0.0,
        help="Optional pause in seconds between GitHub API calls",
    )
    return parser.parse_args()


def parse_utc_boundary(value: str, *, is_end: bool) -> datetime:
    text = value.strip()
    if not text:
        raise RuntimeError("Empty datetime value.")

    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        dt = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
        if is_end:
            dt = dt + timedelta(days=1)
        return dt

    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise RuntimeError(f"Invalid datetime value: {value!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def parse_github_timestamp(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_timestamp_to_utc(value: str) -> str:
    dt = parse_github_timestamp(value)
    if not dt:
        return value.strip()
    return format_utc(dt)


def utc_day(value: str) -> str:
    dt = parse_github_timestamp(value)
    if not dt:
        return value[:10]
    return dt.date().isoformat()


def within_window(timestamp: str, start: datetime, end: datetime) -> bool:
    dt = parse_github_timestamp(timestamp)
    if not dt:
        return False
    return start <= dt < end


def normalize_login(login: str) -> str:
    return login.strip().lstrip("@").lower()


def resolve_token(explicit_token: str) -> Tuple[str, str]:
    if explicit_token.strip():
        return explicit_token.strip(), "--token"

    for env_key in ("GITHUB_TOKEN", "GH_TOKEN"):
        value = os.environ.get(env_key, "").strip()
        if value:
            return value, env_key

    gh_path = shutil_which("gh")
    if gh_path:
        proc = subprocess.run(
            [gh_path, "auth", "token"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        token = proc.stdout.strip()
        if proc.returncode == 0 and token:
            return token, "gh auth token"

    return "", "unauthenticated"


def shutil_which(binary: str) -> Optional[str]:
    path = os.environ.get("PATH", "")
    for folder in path.split(os.pathsep):
        candidate = Path(folder) / binary
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


def extract_repo_full_name(repository_url: str) -> str:
    marker = "/repos/"
    if marker not in repository_url:
        return ""
    return repository_url.split(marker, 1)[1].strip("/")


def pr_key(repo_full_name: str, number: int) -> str:
    return f"{repo_full_name}#{number}"


def int_value(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def fetch_pr_detail(
    client: GitHubClient,
    issue_item: Dict[str, Any],
    cache: Dict[str, PRRecord],
) -> PRRecord:
    pr_data = issue_item.get("pull_request") or {}
    pr_url = pr_data.get("url", "")
    if not pr_url:
        raise RuntimeError("Search item missing pull_request.url")

    if pr_url in cache:
        return cache[pr_url]

    payload, _ = client.request_json(pr_url)

    repo_full_name = ""
    base = payload.get("base") or {}
    repo = base.get("repo") or {}
    if isinstance(repo, dict):
        repo_full_name = (repo.get("full_name") or "").strip()
    if not repo_full_name:
        repo_full_name = extract_repo_full_name(issue_item.get("repository_url", ""))

    number = int_value(payload.get("number"))
    record = PRRecord(
        key=pr_key(repo_full_name, number),
        repo_full_name=repo_full_name,
        number=number,
        title=(payload.get("title") or "").strip(),
        html_url=(payload.get("html_url") or issue_item.get("html_url") or "").strip(),
        author_login=((payload.get("user") or {}).get("login") or "").strip(),
        merged_by_login=((payload.get("merged_by") or {}).get("login") or "").strip(),
        merged_at=normalize_timestamp_to_utc((payload.get("merged_at") or "").strip()),
        additions=int_value(payload.get("additions"), 0),
        deletions=int_value(payload.get("deletions"), 0),
        changed_files=int_value(payload.get("changed_files"), 0),
        commits=int_value(payload.get("commits"), 0),
        merge_commit_sha=(payload.get("merge_commit_sha") or "").strip(),
    )

    cache[pr_url] = record
    return record


def fetch_pr_commit_shas(
    client: GitHubClient,
    repo_full_name: str,
    pr_number: int,
    *,
    max_pages: int,
) -> Set[str]:
    shas: Set[str] = set()
    if not repo_full_name or pr_number <= 0:
        return shas

    endpoint = f"/repos/{repo_full_name}/pulls/{pr_number}/commits"
    page = 1

    while page <= max_pages:
        payload, _ = client.request_json(endpoint, params={"per_page": 100, "page": page})
        if not isinstance(payload, list) or not payload:
            break

        for item in payload:
            sha = (item.get("sha") or "").strip()
            if sha:
                shas.add(sha)

        if len(payload) < 100:
            break

        page += 1

    return shas


def parse_commit_item(item: Dict[str, Any]) -> Optional[CommitRecord]:
    sha = (item.get("sha") or "").strip()
    if not sha:
        return None

    repo = item.get("repository") or {}
    repo_full_name = (repo.get("full_name") or "").strip()

    commit = item.get("commit") or {}
    commit_author = commit.get("author") or {}
    authored_at = normalize_timestamp_to_utc((commit_author.get("date") or "").strip())

    message = (commit.get("message") or "").strip()
    message_title = message.splitlines()[0].strip() if message else ""

    return CommitRecord(
        sha=sha,
        repo_full_name=repo_full_name,
        html_url=(item.get("html_url") or "").strip(),
        api_url=(item.get("url") or "").strip(),
        message_title=message_title,
        authored_at=authored_at,
    )


def fetch_commit_stats(client: GitHubClient, commit: CommitRecord) -> CommitRecord:
    if not commit.api_url:
        return commit

    payload, _ = client.request_json(commit.api_url)
    stats = payload.get("stats") or {}
    files = payload.get("files") or []

    commit.additions = int_value(stats.get("additions"), 0)
    commit.deletions = int_value(stats.get("deletions"), 0)
    if isinstance(files, list):
        commit.changed_files = len(files)
        commit.file_paths = []
        for file_item in files[:200]:
            filename = ""
            if isinstance(file_item, dict):
                filename = (file_item.get("filename") or "").strip()
            if filename:
                commit.file_paths.append(filename)
    else:
        commit.changed_files = 0
        commit.file_paths = []
    return commit


def owner_of_repo(repo_full_name: str) -> str:
    if "/" not in repo_full_name:
        return ""
    return repo_full_name.split("/", 1)[0].lower()


def short_sha(sha: str) -> str:
    return sha[:7] if len(sha) >= 7 else sha


def md_escape(value: Any) -> str:
    text = str(value)
    return text.replace("|", "\\|").replace("\n", " ").strip()


def render_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> List[str]:
    if not rows:
        return ["_No records._"]

    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]

    for row in rows:
        lines.append("| " + " | ".join(md_escape(cell) for cell in row) + " |")

    return lines


def to_markdown_link(text: str, url: str) -> str:
    if not url:
        return text
    return f"[{text}]({url})"


def _contains_keyword(text: str, keywords: Sequence[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def _path_ext(path: str) -> str:
    name = path.rsplit("/", 1)[-1]
    if "." not in name:
        return ""
    return name.rsplit(".", 1)[-1].lower()


def infer_work_category(text: str, file_paths: Sequence[str]) -> str:
    normalized = f" {text.lower().strip()} "
    paths = [path.lower() for path in file_paths]
    joined_paths = " ".join(paths)
    path_exts = {_path_ext(path) for path in paths}

    if _contains_keyword(
        joined_paths,
        (
            ".github/workflows",
            "dockerfile",
            "docker-compose",
            "k8s",
            "kubernetes",
            "helm",
            "terraform",
            "infra/",
            "deploy",
            "deployment",
        ),
    ) or _contains_keyword(
        normalized,
        (" ci ", " cd ", " pipeline ", " deploy ", " infra ", " devops ", " docker ", " k8s "),
    ):
        return "Infrastructure and CI"

    if _contains_keyword(joined_paths, ("test/", "/test", "__tests__", "_test.", ".spec.")) or _contains_keyword(
        normalized,
        (" test ", " tests ", " unittest ", " integration ", " e2e ", " qa "),
    ):
        return "Testing and Quality"

    if _contains_keyword(
        joined_paths,
        ("requirements.txt", "poetry.lock", "package-lock.json", "pnpm-lock.yaml", "yarn.lock", "cargo.lock", "go.mod", "go.sum"),
    ) or _contains_keyword(
        normalized,
        (" dependency ", " dependencies ", " bump ", " upgrade ", " security ", " cve ", " vuln ", " vulnerability "),
    ):
        return "Dependency and Security"

    if _contains_keyword(joined_paths, ("docs/", "doc/", "readme", "changelog", "wiki/")) or path_exts & {
        "md",
        "rst",
        "adoc",
    }:
        if _contains_keyword(
            normalized, (" feat ", " feature ", " fix ", " bug ", " refactor ", " optimize ", " performance ")
        ):
            pass
        else:
            return "Documentation"

    if _contains_keyword(
        joined_paths,
        ("data/", "dataset", "archive/", "report/", "reports/", "assets/", "notebook", ".ipynb"),
    ) or path_exts & {"csv", "tsv", "xlsx", "xls", "jsonl", "parquet", "pdf", "docx", "pptx"}:
        if _contains_keyword(normalized, (" feat ", " feature ", " fix ", " bug ", " refactor ", " test ")):
            pass
        else:
            return "Data and Content Updates"

    if _contains_keyword(normalized, (" release ", " version ", " tag ", "chore(release)", " semver ")):
        return "Release and Versioning"

    if _contains_keyword(
        normalized,
        (" fix ", " bug ", " hotfix ", " resolve ", " patch ", " correct ", " rollback ", " revert "),
    ):
        return "Bug Fixes"

    if _contains_keyword(
        normalized,
        (" refactor ", " cleanup ", " clean up ", " simplify ", " optimize ", " optimization ", " restructure "),
    ):
        return "Refactor and Cleanup"

    if _contains_keyword(
        normalized,
        (
            " feat ",
            " feature ",
            " implement ",
            " add ",
            " create ",
            " support ",
            " enable ",
            " introduce ",
            "build ",
        ),
    ):
        return "Feature Development"

    if _contains_keyword(normalized, (" doc ", " docs ", " readme ", " guide ", " tutorial ")):
        return "Documentation"

    return "General Engineering Work"


def code_delta_text(additions: Optional[int], deletions: Optional[int]) -> str:
    if additions is None and deletions is None:
        return "n/a"
    return f"+{(additions or 0):,} / -{(deletions or 0):,}"


def clip_text(text: str, limit: int = 120) -> str:
    collapsed = re.sub(r"\s+", " ", text or "").strip()
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: max(0, limit - 3)].rstrip() + "..."


def normalize_change_text(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", text or "").strip()
    # Remove conventional-commit prefixes, for example `feat:`, `fix(core):`.
    cleaned = re.sub(r"^[a-z]+(?:\([^)]+\))?!?:\s*", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def is_generic_change_text(text: str) -> bool:
    lowered = (text or "").strip().lower()
    if not lowered:
        return True
    if re.fullmatch(r"[0-9_.-]+", lowered):
        return True
    generic = {
        "update",
        "updates",
        "updated",
        "fix",
        "bug fix",
        "fixes",
        "refactor",
        "cleanup",
        "chore",
        "generated",
        "wip",
        "minor update",
        "sync",
        "test",
    }
    if lowered in generic:
        return True
    if len(lowered) <= 5:
        return True
    return False


def summarize_paths(paths: Sequence[str], *, max_items: int = 3) -> str:
    compact: List[str] = []
    seen: Set[str] = set()
    for path in paths:
        normalized = path.strip().lstrip("./")
        if not normalized:
            continue
        parts = normalized.split("/")
        if len(parts) >= 2:
            token = f"{parts[0]}/{parts[1]}"
        else:
            token = parts[0]
        if token in seen:
            continue
        seen.add(token)
        compact.append(token)
        if len(compact) >= max_items:
            break
    return ", ".join(compact)


def work_type_label(item: WorkItem) -> str:
    if item.kind == "Maintainer Merge":
        return "Maintainer Merge"

    mapping = {
        "Feature Development": "Added Feature",
        "Bug Fixes": "Bug Fix",
        "Refactor and Cleanup": "Refactor/Cleanup",
        "Infrastructure and CI": "Infra/CI Update",
        "Testing and Quality": "Testing/Quality",
        "Dependency and Security": "Dependency/Security",
        "Documentation": "Documentation Update",
        "Data and Content Updates": "Data/Content Update",
        "Release and Versioning": "Release/Version",
        "General Engineering Work": "General Update",
        "Maintainer Merge Work": "Maintainer Merge",
    }
    return mapping.get(item.category, "General Update")


def concrete_change_text(item: WorkItem) -> str:
    text = normalize_change_text(item.content)
    if text and not is_generic_change_text(text):
        return text

    path_summary = summarize_paths(item.file_paths or [])
    if path_summary:
        return f"Updated files/modules in {path_summary}"

    if item.changed_files is not None and item.changed_files > 0:
        return f"Updated {item.changed_files} files"

    fallback = {
        "Feature Development": "Implemented feature updates",
        "Bug Fixes": "Fixed defects and stabilized behavior",
        "Refactor and Cleanup": "Refactored code and cleaned up structure",
        "Infrastructure and CI": "Updated infrastructure or CI workflows",
        "Testing and Quality": "Improved tests or quality checks",
        "Dependency and Security": "Updated dependencies or security-related components",
        "Documentation": "Updated documentation or written materials",
        "Data and Content Updates": "Updated datasets or content artifacts",
        "Release and Versioning": "Updated release/version metadata",
        "Maintainer Merge Work": "Merged contributor PRs as maintainer",
        "General Engineering Work": "Completed engineering updates",
    }
    return fallback.get(item.category, "Completed engineering updates")


def work_item_score(item: WorkItem) -> int:
    added = (item.additions or 0) if item.additions is not None else 0
    removed = (item.deletions or 0) if item.deletions is not None else 0
    files = (item.changed_files or 0) if item.changed_files is not None else 0
    magnitude = added + removed + files * 20

    if item.kind == "Authored Merged PR":
        return magnitude + 800
    if item.kind == "Maintainer Merge":
        return magnitude + 600
    return magnitude + 300


def build_work_items(
    authored_prs: Sequence[PRRecord],
    merged_by_non_self: Sequence[PRRecord],
    direct_commits: Sequence[CommitRecord],
) -> List[WorkItem]:
    items: List[WorkItem] = []

    for pr in authored_prs:
        items.append(
            WorkItem(
                date_utc=utc_day(pr.merged_at),
                kind="Authored Merged PR",
                category=infer_work_category(pr.title, []),
                repo_full_name=pr.repo_full_name,
                evidence_text=f"PR #{pr.number}",
                evidence_url=pr.html_url,
                content=pr.title,
                additions=pr.additions,
                deletions=pr.deletions,
                changed_files=pr.changed_files,
            )
        )

    for pr in merged_by_non_self:
        items.append(
            WorkItem(
                date_utc=utc_day(pr.merged_at),
                kind="Maintainer Merge",
                category="Maintainer Merge Work",
                repo_full_name=pr.repo_full_name,
                evidence_text=f"PR #{pr.number}",
                evidence_url=pr.html_url,
                content=f"{pr.title} (author: @{pr.author_login or 'unknown'})",
                additions=pr.additions,
                deletions=pr.deletions,
                changed_files=pr.changed_files,
            )
        )

    for commit in direct_commits:
        items.append(
            WorkItem(
                date_utc=utc_day(commit.authored_at),
                kind="Direct Commit",
                category=infer_work_category(commit.message_title, commit.file_paths or []),
                repo_full_name=commit.repo_full_name,
                evidence_text=short_sha(commit.sha),
                evidence_url=commit.html_url,
                content=commit.message_title,
                additions=commit.additions,
                deletions=commit.deletions,
                changed_files=commit.changed_files,
                file_paths=commit.file_paths or [],
            )
        )

    return items


def build_report(
    *,
    target_user: str,
    start: datetime,
    end: datetime,
    generated_at: datetime,
    auth_source: str,
    viewer_login: str,
    scope_header: str,
    scope_has_repo: bool,
    authored_prs: List[PRRecord],
    merged_by_prs: List[PRRecord],
    merged_by_non_self: List[PRRecord],
    all_commits: List[CommitRecord],
    direct_commits: List[CommitRecord],
    commit_stats_covered: int,
    search_metas: List[SearchMeta],
    warnings: List[str],
    max_table_rows: int,
    max_repo_detail_rows: int,
) -> str:
    repos_touched: Set[str] = set()
    for pr in authored_prs:
        if pr.repo_full_name:
            repos_touched.add(pr.repo_full_name)
    for pr in merged_by_prs:
        if pr.repo_full_name:
            repos_touched.add(pr.repo_full_name)
    for commit in direct_commits:
        if commit.repo_full_name:
            repos_touched.add(commit.repo_full_name)

    own_repos = {repo for repo in repos_touched if owner_of_repo(repo) == target_user}
    other_repos = repos_touched - own_repos

    authored_pr_add = sum(pr.additions for pr in authored_prs)
    authored_pr_del = sum(pr.deletions for pr in authored_prs)

    direct_commit_add = sum((c.additions or 0) for c in direct_commits if c.additions is not None)
    direct_commit_del = sum((c.deletions or 0) for c in direct_commits if c.deletions is not None)

    repo_stats: Dict[str, Dict[str, int]] = defaultdict(
        lambda: {
            "authored_merged_pr": 0,
            "merged_others_pr": 0,
            "direct_commits": 0,
            "pr_additions": 0,
            "pr_deletions": 0,
            "commit_additions": 0,
            "commit_deletions": 0,
        }
    )

    for pr in authored_prs:
        stats = repo_stats[pr.repo_full_name]
        stats["authored_merged_pr"] += 1
        stats["pr_additions"] += pr.additions
        stats["pr_deletions"] += pr.deletions

    for pr in merged_by_non_self:
        stats = repo_stats[pr.repo_full_name]
        stats["merged_others_pr"] += 1

    for commit in direct_commits:
        stats = repo_stats[commit.repo_full_name]
        stats["direct_commits"] += 1
        if commit.additions is not None:
            stats["commit_additions"] += commit.additions
        if commit.deletions is not None:
            stats["commit_deletions"] += commit.deletions

    timeline: Dict[str, Dict[str, int]] = defaultdict(
        lambda: {"authored_merged_pr": 0, "merged_others_pr": 0, "direct_commits": 0}
    )

    for pr in authored_prs:
        day = utc_day(pr.merged_at)
        timeline[day]["authored_merged_pr"] += 1

    for pr in merged_by_non_self:
        day = utc_day(pr.merged_at)
        timeline[day]["merged_others_pr"] += 1

    for commit in direct_commits:
        day = utc_day(commit.authored_at)
        timeline[day]["direct_commits"] += 1

    work_items = build_work_items(authored_prs, merged_by_non_self, direct_commits)

    category_stats: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "count": 0,
            "repos": set(),
            "additions": 0,
            "deletions": 0,
            "examples": [],
        }
    )
    for item in work_items:
        stats = category_stats[item.category]
        stats["count"] += 1
        stats["repos"].add(item.repo_full_name)
        stats["additions"] += (item.additions or 0) if item.additions is not None else 0
        stats["deletions"] += (item.deletions or 0) if item.deletions is not None else 0
        examples: List[WorkItem] = stats["examples"]
        examples.append(item)
        examples.sort(key=work_item_score, reverse=True)
        del examples[2:]

    sorted_categories = sorted(
        category_stats.items(),
        key=lambda kv: (
            kv[1]["additions"] + kv[1]["deletions"] + kv[1]["count"] * 300,
            kv[1]["count"],
        ),
        reverse=True,
    )

    top_work_items = sorted(work_items, key=work_item_score, reverse=True)

    lines: List[str] = []
    lines.append(f"# GitHub Contribution Report: @{target_user}")
    lines.append("")
    lines.append("## Analysis Scope")
    lines.append(f"- Target user: `@{target_user}`")
    lines.append(f"- Window (UTC, left-closed right-open): `{format_utc(start)} -> {format_utc(end)}`")
    lines.append(f"- Generated at (UTC): `{format_utc(generated_at)}`")
    lines.append(f"- Auth source: `{auth_source}`")
    if viewer_login:
        lines.append(f"- Authenticated GitHub identity: `@{viewer_login}`")
    else:
        lines.append("- Authenticated GitHub identity: `_none (unauthenticated)_`")
    if scope_header:
        lines.append(f"- OAuth scopes: `{scope_header}`")
    else:
        lines.append("- OAuth scopes: `_not available_`")
    lines.append(
        "- Private repository coverage: "
        + ("`enabled (repo scope detected)`" if scope_has_repo else "`unknown or limited`")
    )
    lines.append(
        "- Included contribution types: `authored merged PR`, `PR merged by target user`, `authored commit`"
    )
    lines.append("- Excluded types: `comment`, `review`, `star`, `watch`, `fork`")
    lines.append("")

    lines.append("## Summary")
    lines.append(f"- Authored merged PRs: **{len(authored_prs)}**")
    lines.append(f"- PRs merged by target user: **{len(merged_by_prs)}**")
    lines.append(f"- PRs merged by target user for other authors: **{len(merged_by_non_self)}**")
    lines.append(f"- Authored commits (all): **{len(all_commits)}**")
    lines.append(
        "- Direct commits (excluding commits inside authored merged PRs): "
        f"**{len(direct_commits)}**"
    )
    lines.append(f"- Repositories touched: **{len(repos_touched)}**")
    lines.append(f"- Own repositories touched: **{len(own_repos)}**")
    lines.append(f"- Other repositories touched: **{len(other_repos)}**")
    lines.append(
        f"- Authored merged PR code delta: `+{authored_pr_add:,}` / `-{authored_pr_del:,}`"
    )
    lines.append(
        f"- Direct commit code delta (stats coverage {commit_stats_covered}/{len(direct_commits)}): "
        f"`+{direct_commit_add:,}` / `-{direct_commit_del:,}`"
    )
    lines.append("")

    lines.append("## Substantive Contribution Work")
    if not work_items:
        lines.append("- No practical contribution records were found in this time window.")
    else:
        category_limit = max(1, min(8, max_table_rows))
        for category, stats in sorted_categories[:category_limit]:
            example_links = ", ".join(
                to_markdown_link(example.evidence_text, example.evidence_url)
                for example in stats["examples"]
            )
            lines.append(
                f"- **{category}**: {stats['count']} items across {len(stats['repos'])} repos, "
                f"code delta `{code_delta_text(stats['additions'], stats['deletions'])}`; "
                f"examples: {example_links if example_links else 'n/a'}"
            )
    lines.append("")

    lines.append("## Work Content Highlights")
    highlight_rows = []
    for item in top_work_items[:max_table_rows]:
        highlight_rows.append(
            (
                item.date_utc,
                item.kind,
                item.category,
                item.repo_full_name,
                to_markdown_link(item.evidence_text, item.evidence_url),
                clip_text(item.content),
                code_delta_text(item.additions, item.deletions),
                item.changed_files if item.changed_files is not None else "n/a",
            )
        )
    lines.extend(
        render_table(
            ["Date", "Kind", "Category", "Repo", "Evidence", "Work Content", "Code Delta", "Files"],
            highlight_rows,
        )
    )
    lines.append("")

    lines.append("## Data Coverage Notes")
    for meta in search_metas:
        lines.append(
            f"- Query `{meta.query}` -> fetched `{meta.fetched}` of reported `{meta.total_count}`"
            f" (cap `{meta.cap}`, incomplete_results={meta.incomplete_results})"
        )
    lines.append(
        "- Search API hard-limit is 1000 results per query. Increase precision by splitting the time range if needed."
    )
    lines.append("")

    if warnings:
        lines.append("## Warnings")
        for warning in warnings:
            lines.append(f"- {warning}")
        lines.append("")

    lines.append("## Repository Breakdown")
    repo_rows: List[Tuple[Any, ...]] = []
    for repo_name, stats in repo_stats.items():
        score = (
            stats["authored_merged_pr"] * 100
            + stats["merged_others_pr"] * 60
            + stats["direct_commits"] * 10
            + stats["pr_additions"]
            + stats["commit_additions"]
        )
        repo_rows.append(
            (
                score,
                repo_name,
                stats["authored_merged_pr"],
                stats["merged_others_pr"],
                stats["direct_commits"],
                f"+{stats['pr_additions'] + stats['commit_additions']:,} / -{stats['pr_deletions'] + stats['commit_deletions']:,}",
                "own" if owner_of_repo(repo_name) == target_user else "other",
            )
        )

    repo_rows.sort(key=lambda row: row[0], reverse=True)
    lines.extend(
        render_table(
            ["Repository", "Authored Merged PR", "Merged Others PR", "Direct Commits", "Code Delta", "Type"],
            [row[1:] for row in repo_rows[:max_table_rows]],
        )
    )
    lines.append("")

    repo_work_items: Dict[str, List[WorkItem]] = defaultdict(list)
    for item in work_items:
        if item.repo_full_name:
            repo_work_items[item.repo_full_name].append(item)

    lines.append("## Repository Change Details")
    if not repo_rows:
        lines.append("_No repositories with practical contribution records._")
        lines.append("")
    else:
        detail_row_limit = max(1, max_repo_detail_rows)
        for row in repo_rows:
            repo_name = row[1]
            stats = repo_stats[repo_name]
            repo_items = sorted(repo_work_items.get(repo_name, []), key=work_item_score, reverse=True)

            lines.append(f"### {repo_name}")
            lines.append(
                "- Contribution summary: "
                f"`{stats['authored_merged_pr']}` authored merged PR, "
                f"`{stats['merged_others_pr']}` maintainer merges, "
                f"`{stats['direct_commits']}` direct commits, "
                f"code delta `+{stats['pr_additions'] + stats['commit_additions']:,} / "
                f"-{stats['pr_deletions'] + stats['commit_deletions']:,}`."
            )

            if not repo_items:
                lines.append("_No concrete change rows available._")
                lines.append("")
                continue

            detail_rows: List[Tuple[Any, ...]] = []
            seen_change_keys: Set[str] = set()
            for item in repo_items:
                change_text = concrete_change_text(item)
                change_key = f"{work_type_label(item)}::{change_text.lower()}"
                if change_key in seen_change_keys:
                    continue
                seen_change_keys.add(change_key)
                detail_rows.append(
                    (
                        item.date_utc,
                        work_type_label(item),
                        clip_text(change_text, 110),
                        to_markdown_link(item.evidence_text, item.evidence_url),
                        code_delta_text(item.additions, item.deletions),
                        item.changed_files if item.changed_files is not None else "n/a",
                    )
                )
                if len(detail_rows) >= detail_row_limit:
                    break

            lines.extend(
                render_table(
                    ["Date", "Change Type", "Concrete Change", "Evidence", "Code Delta", "Files"],
                    detail_rows,
                )
            )
            lines.append("")

    lines.append("## Contribution Timeline (UTC)")
    timeline_rows: List[Tuple[str, int, int, int]] = []
    for day, counts in timeline.items():
        timeline_rows.append(
            (
                day,
                counts["authored_merged_pr"],
                counts["merged_others_pr"],
                counts["direct_commits"],
            )
        )
    timeline_rows.sort(key=lambda row: row[0])
    lines.extend(
        render_table(
            ["Date", "Authored Merged PR", "Merged Others PR", "Direct Commits"],
            timeline_rows[:max_table_rows],
        )
    )
    lines.append("")

    lines.append("## Top Authored Merged PRs")
    authored_sorted = sorted(authored_prs, key=lambda pr: pr.additions + pr.deletions, reverse=True)
    authored_rows = []
    for pr in authored_sorted[:max_table_rows]:
        authored_rows.append(
            (
                pr.merged_at[:10],
                # `merged_at` is already UTC normalized.
                pr.repo_full_name,
                to_markdown_link(f"#{pr.number}", pr.html_url),
                f"+{pr.additions:,} / -{pr.deletions:,}",
                pr.changed_files,
                pr.commits,
                pr.title,
            )
        )
    lines.extend(
        render_table(
            ["Merged Date", "Repo", "PR", "Code Delta", "Files", "Commits", "Title"],
            authored_rows,
        )
    )
    lines.append("")

    lines.append("## Top Maintainer Merges (Merged Others' PRs)")
    merged_other_rows = []
    merged_other_sorted = sorted(
        merged_by_non_self,
        key=lambda pr: (parse_github_timestamp(pr.merged_at) or datetime.min.replace(tzinfo=timezone.utc)),
        reverse=True,
    )
    for pr in merged_other_sorted[:max_table_rows]:
        merged_other_rows.append(
            (
                pr.merged_at[:10],
                # `merged_at` is already UTC normalized.
                pr.repo_full_name,
                to_markdown_link(f"#{pr.number}", pr.html_url),
                pr.author_login or "unknown",
                f"+{pr.additions:,} / -{pr.deletions:,}",
                pr.title,
            )
        )
    lines.extend(
        render_table(
            ["Merged Date", "Repo", "PR", "PR Author", "Code Delta", "Title"],
            merged_other_rows,
        )
    )
    lines.append("")

    lines.append("## Top Direct Commits")
    commit_sorted = sorted(
        direct_commits,
        key=lambda c: ((c.additions or 0) + (c.deletions or 0), parse_github_timestamp(c.authored_at) or datetime.min.replace(tzinfo=timezone.utc)),
        reverse=True,
    )
    commit_rows = []
    for commit in commit_sorted[:max_table_rows]:
        if commit.additions is None or commit.deletions is None:
            delta = "n/a"
        else:
            delta = f"+{commit.additions:,} / -{commit.deletions:,}"
        commit_rows.append(
            (
                utc_day(commit.authored_at),
                commit.repo_full_name,
                to_markdown_link(short_sha(commit.sha), commit.html_url),
                delta,
                commit.changed_files if commit.changed_files is not None else "n/a",
                commit.message_title,
            )
        )
    lines.extend(
        render_table(["Date", "Repo", "Commit", "Code Delta", "Files", "Message"], commit_rows)
    )
    lines.append("")

    lines.append("## Evidence Appendix")
    lines.append("### Authored Merged PR URLs")
    if authored_prs:
        for pr in sorted(authored_prs, key=lambda x: x.merged_at, reverse=True)[: max_table_rows * 3]:
            lines.append(f"- {pr.repo_full_name} #{pr.number}: {pr.html_url}")
        if len(authored_prs) > max_table_rows * 3:
            lines.append(f"- ... truncated, showing {max_table_rows * 3} of {len(authored_prs)}")
    else:
        lines.append("- none")

    lines.append("")
    lines.append("### Direct Commit URLs")
    if direct_commits:
        for commit in sorted(direct_commits, key=lambda x: x.authored_at, reverse=True)[: max_table_rows * 3]:
            lines.append(f"- {commit.repo_full_name} {short_sha(commit.sha)}: {commit.html_url}")
        if len(direct_commits) > max_table_rows * 3:
            lines.append(f"- ... truncated, showing {max_table_rows * 3} of {len(direct_commits)}")
    else:
        lines.append("- none")

    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()

    target_user = normalize_login(args.user)
    if not target_user:
        raise RuntimeError("--user must not be empty.")

    start = parse_utc_boundary(args.start, is_end=False)
    end = parse_utc_boundary(args.end, is_end=True)
    if start >= end:
        raise RuntimeError("--start must be earlier than --end.")

    token, auth_source = resolve_token(args.token)
    client = GitHubClient(token=token, request_pause=args.request_pause)

    warnings: List[str] = []
    viewer_login = ""
    scope_header = ""
    scope_has_repo = False

    if token:
        user_payload, headers = client.request_json("/user")
        viewer_login = (user_payload.get("login") or "").strip()
        scope_header = (headers.get("x-oauth-scopes") or "").strip()
        scopes = {scope.strip() for scope in scope_header.split(",") if scope.strip()}
        scope_has_repo = "repo" in scopes
        if not scope_has_repo:
            warnings.append(
                "Token scope does not explicitly include 'repo'; private repository coverage may be incomplete."
            )
    else:
        warnings.append(
            "No GitHub token found. Running unauthenticated; private repositories and many accessible repos are excluded."
        )

    start_date = start.date().isoformat()
    end_inclusive_date = (end - timedelta(seconds=1)).date().isoformat()

    authored_pr_query = f"is:pr author:{target_user} merged:{start_date}..{end_inclusive_date} archived:false"
    merged_by_query = f"is:pr merged-by:{target_user} merged:{start_date}..{end_inclusive_date} archived:false"
    commit_query = f"author:{target_user} author-date:{start_date}..{end_inclusive_date} merge:false"

    authored_issue_items, authored_meta = client.search(
        "/search/issues",
        authored_pr_query,
        max_items=max(args.max_prs, 0),
        accept=DEFAULT_ACCEPT,
        sort="updated",
        order="desc",
    )
    merged_by_issue_items, merged_by_meta = client.search(
        "/search/issues",
        merged_by_query,
        max_items=max(args.max_prs, 0),
        accept=DEFAULT_ACCEPT,
        sort="updated",
        order="desc",
    )
    commit_items, commit_meta = client.search(
        "/search/commits",
        commit_query,
        max_items=max(args.max_commits, 0),
        accept=COMMIT_SEARCH_ACCEPT,
        sort="author-date",
        order="desc",
    )

    if authored_meta.incomplete_results:
        warnings.append("Authored merged PR search returned incomplete_results=true.")
    if merged_by_meta.incomplete_results:
        warnings.append("Merged-by PR search returned incomplete_results=true.")
    if commit_meta.incomplete_results:
        warnings.append("Commit search returned incomplete_results=true.")

    pr_cache: Dict[str, PRRecord] = {}

    authored_pr_map: Dict[str, PRRecord] = {}
    for item in authored_issue_items:
        try:
            pr = fetch_pr_detail(client, item, pr_cache)
        except Exception as exc:  # pylint: disable=broad-except
            warnings.append(f"Failed to load authored PR detail from {item.get('html_url', '')}: {exc}")
            continue

        if not pr.merged_at or not within_window(pr.merged_at, start, end):
            continue

        authored_pr_map[pr.key] = pr

    merged_by_map: Dict[str, PRRecord] = {}
    for item in merged_by_issue_items:
        try:
            pr = fetch_pr_detail(client, item, pr_cache)
        except Exception as exc:  # pylint: disable=broad-except
            warnings.append(f"Failed to load merged-by PR detail from {item.get('html_url', '')}: {exc}")
            continue

        if not pr.merged_at or not within_window(pr.merged_at, start, end):
            continue

        merged_by_map[pr.key] = pr

    authored_prs = sorted(
        authored_pr_map.values(),
        key=lambda x: parse_github_timestamp(x.merged_at) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    merged_by_prs = sorted(
        merged_by_map.values(),
        key=lambda x: parse_github_timestamp(x.merged_at) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    merged_by_non_self = [
        pr for pr in merged_by_prs if normalize_login(pr.author_login) != target_user
    ]

    authored_pr_commit_shas: Set[str] = set()
    if args.no_pr_commit_dedupe:
        warnings.append("PR-commit dedupe is disabled by --no-pr-commit-dedupe.")
    else:
        for pr in authored_prs:
            try:
                shas = fetch_pr_commit_shas(
                    client,
                    pr.repo_full_name,
                    pr.number,
                    max_pages=max(args.max_pr_commit_pages, 1),
                )
                authored_pr_commit_shas.update(shas)
            except Exception as exc:  # pylint: disable=broad-except
                warnings.append(
                    f"Failed to load PR commit SHAs for {pr.repo_full_name}#{pr.number}: {exc}"
                )

    seen_commit_shas: Set[str] = set()
    all_commits: List[CommitRecord] = []

    for item in commit_items:
        commit = parse_commit_item(item)
        if not commit:
            continue
        if commit.sha in seen_commit_shas:
            continue
        seen_commit_shas.add(commit.sha)

        if not commit.authored_at or not within_window(commit.authored_at, start, end):
            continue

        all_commits.append(commit)

    all_commits.sort(
        key=lambda x: parse_github_timestamp(x.authored_at) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    if args.no_pr_commit_dedupe:
        direct_commits = list(all_commits)
    else:
        direct_commits = [commit for commit in all_commits if commit.sha not in authored_pr_commit_shas]

    commit_stats_covered = 0
    detail_budget = max(0, min(args.max_commit_detail, len(direct_commits)))
    for idx in range(detail_budget):
        commit = direct_commits[idx]
        try:
            fetch_commit_stats(client, commit)
            commit_stats_covered += 1
        except Exception as exc:  # pylint: disable=broad-except
            warnings.append(
                f"Failed to load commit detail for {commit.repo_full_name} {short_sha(commit.sha)}: {exc}"
            )

    report = build_report(
        target_user=target_user,
        start=start,
        end=end,
        generated_at=datetime.now(timezone.utc),
        auth_source=auth_source,
        viewer_login=viewer_login,
        scope_header=scope_header,
        scope_has_repo=scope_has_repo,
        authored_prs=authored_prs,
        merged_by_prs=merged_by_prs,
        merged_by_non_self=merged_by_non_self,
        all_commits=all_commits,
        direct_commits=direct_commits,
        commit_stats_covered=commit_stats_covered,
        search_metas=[authored_meta, merged_by_meta, commit_meta],
        warnings=warnings,
        max_table_rows=max(1, args.max_table_rows),
        max_repo_detail_rows=max(1, args.max_repo_detail_rows),
    )

    if args.output == "-":
        print(report)
    else:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report, encoding="utf-8")
        print(f"Wrote Markdown report: {output_path}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
