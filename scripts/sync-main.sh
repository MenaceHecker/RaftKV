#!/usr/bin/env bash
# Merge origin/main into this branch and refresh the README counts (make sync-main).
#
# The README test counts live on two lines that every PR adding tests edits, so merging
# main often conflicts there. If README.md is the only conflicted file, this merges the
# three versions with the number sites masked (so the counts cannot conflict) and then
# writes the real counts. It never pushes. A clean merge is committed by git as usual; a
# README-only conflict is resolved and the merge is completed; a refreshed README after a
# clean merge is left as an uncommitted change for you to commit.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

git fetch origin
if ! git merge --no-edit origin/main; then
  conflicted=$(git diff --name-only --diff-filter=U)
  if [ "$conflicted" != "README.md" ]; then
    echo "sync-main: conflicts in files other than README.md; resolve them by hand:" >&2
    echo "$conflicted" >&2
    exit 1
  fi
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  mask() { go run ./cmd/readmecounts -mask; }
  git show :1:README.md | mask > "$tmp/base"
  git show :2:README.md | mask > "$tmp/ours"
  git show :3:README.md | mask > "$tmp/theirs"
  if ! git merge-file -p "$tmp/ours" "$tmp/base" "$tmp/theirs" > "$tmp/merged"; then
    echo "sync-main: README.md has conflicts beyond the counts; resolve them by hand." >&2
    exit 1
  fi
  cp "$tmp/merged" README.md
  go run ./cmd/readmecounts
  git add README.md
  git commit --no-edit
else
  go run ./cmd/readmecounts
fi
git status --short
