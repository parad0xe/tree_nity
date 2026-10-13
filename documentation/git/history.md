# History

Rules for updating and cleaning up a branch.

## Requirements

### MUST

- A branch MUST be updated from `main` by rebase or fast-forward.
- A rewritten branch MUST be pushed with `--force-with-lease`.

### MUST NOT

- A branch MUST NOT receive a merge commit from `main`.
- A shared branch MUST NOT be rewritten without the agreement of every
  contributor working on it.
- `main` MUST NOT be rewritten.

## Updating a branch

Rebase the branch onto the latest `main`:

```bash
git switch <branch>
git pull --rebase origin main
```

Rebase a branch created from another branch onto the latest `main`:

```bash
git fetch origin
git rebase --onto origin/main <old-base> <branch>
```

Push the rewritten branch:

```bash
git push --force-with-lease
```

## Cleaning up commits

Fold a correction into an earlier commit:

```bash
git commit --fixup=<commit>
git rebase -i --autosquash <base>
```

Reword, reorder, or edit earlier commits:

```bash
git rebase -i <base>
```
