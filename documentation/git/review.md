# Review

Rules for reviewing a change before merging.

## Requirements

### MUST

- A review MUST cover the complete
  [review range](#establishing-the-review-range).
- A review MUST check the branch against the [branch rules](branches.md).
- A review MUST check every commit against the [commit rules](commits.md).
- A review MUST check that the diff contains no generated binary, editor file,
  credential, or log.
- A review MUST check that the diff introduces no undocumented interface or
  behavior change.
- A review MUST check that tests cover the changed behavior.
- A review MUST check that documentation matches the changed code and
  commands.
- A review MUST record failures, skipped checks, and missing tests.

## Recommendations

### SHOULD

- A review SHOULD be repeated when the branch changes before merging.

## Establishing the review range

Inspect the branch and its commits:

```bash
git status --short --branch
git log --oneline --decorate main..HEAD
git diff --check main...HEAD
git diff main...HEAD
```

Inspect uncommitted changes:

```bash
git diff
git diff --cached
```
