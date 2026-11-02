set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

sync:
    uv sync --locked --all-groups

test:
    uv run pytest

lint:
    uv run ruff check .

format-check:
    uv run ruff format --check .

format:
    uv run ruff format .

check: sync lint format-check test

coverage: sync
    uv run pytest --cov --cov-report=term-missing --cov-report=html --cov-report=xml

build: check
    uv build

release-check: build

release-tag tag: release-check
    test "$(git branch --show-current)" = "main" || { echo "Release tags must be created from main." >&2; exit 1; }
    test -z "$(git status --porcelain)" || { echo "Commit or stash worktree changes before tagging." >&2; exit 1; }
    git rev-parse -q --verify "refs/tags/{{tag}}" >/dev/null && { echo "Tag {{tag}} already exists." >&2; exit 1; } || :
    git tag -a "{{tag}}" -m "Release {{tag}}"
    git push origin "{{tag}}"
