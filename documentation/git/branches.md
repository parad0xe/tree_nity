# Branches

Rules for creating and naming branches.

## Requirements

### MUST

- A feature, fix, or significant change MUST be developed on its own branch.
- A branch MUST contain a single objective.
- A branch name MUST follow the [naming format](#naming-format).
- A change committed directly on `main` MUST be a small non-breaking fix, such
  as a typo, documentation, or trivial correction, that does not alter feature
  logic.

## Naming format

```text
<type>/<name>
```

- `<type>` matches the [commit type](commits.md#types) of the main change:
  `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `style`, `build`, `ci`,
  `chore`, or `revert`.
- `<name>` describes the objective in lowercase words separated by hyphens.

Examples:

```text
feat/user-login
fix/session-timeout
docs/git-workflow
```

## Creating a branch

Create the branch from the latest `main`:

```bash
git switch main
git pull --ff-only
git switch -c <type>/<name>
```
