# Merges

Rules for integrating a branch into `main`.

## Requirements

### MUST

- A branch MUST be [up to date with `main`](history.md#updating-a-branch)
  before merging.
- A branch MUST pass the required CI checks before merging.
- A branch MUST pass the [review](review.md) before merging.
- A branch MUST be merged into `main` with a merge commit, without
  fast-forward.

### MUST NOT

- A branch MUST NOT be squash-merged or rebase-merged.

## Recommendations

### SHOULD

- A branch SHOULD be approved by another contributor before merging.
- A merged branch SHOULD be deleted locally and remotely.
- The hosting platform SHOULD allow merge commits only.

## Merging a branch

Merge the branch into `main`:

```bash
git switch main
git merge --no-ff <branch>
```

Delete the merged branch:

```bash
git branch -d <branch>
git push origin --delete <branch>
```
