# Merges

Rules for integrating a branch into the integration branch.

## Requirements

### MUST

- A branch MUST be
  [up to date with the integration branch](history.md#updating-a-branch) before
  merging.
- A branch MUST pass the required CI checks before merging.
- A branch MUST pass the [review](review.md) before merging.
- A branch MUST be merged into the integration branch with a merge commit,
  without fast-forward.

### MUST NOT

- A branch MUST NOT be squash-merged or rebase-merged.

## Recommendations

### SHOULD

- A branch SHOULD be approved by another contributor before merging.
- A merged branch SHOULD be deleted locally and remotely.
- The hosting platform SHOULD allow merge commits only.

## Commit message

A merge commit may use Git's generated merge message or the regular
[commit message format](commits.md#format).

## Merging a branch

Merge the branch into the integration branch:

```bash
git switch <integration-branch>
git merge --no-ff <branch>
```

Delete the merged branch:

```bash
git branch -d <branch>
git push <remote> --delete <branch>
```
