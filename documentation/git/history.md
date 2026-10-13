# Git History

History must remain understandable, preserve meaningful development steps, and
show the required relationship between feature branches and `main`.

## Interactive rebase and fixups

Interactive rebase is appropriate for cleaning up local, unshared commits:

```bash
git rebase -i HEAD~<count>
```

Common actions are `reword`, `edit`, `squash`, `fixup`, and `drop`. Use `reword`
for an inaccurate message, `edit` to correct a commit's contents, and `fixup`
for a temporary correction that should be folded into its target commit.

A fixup workflow can be prepared with:

```bash
git commit --fixup=<target-commit>
git rebase -i --autosquash <base-commit>
```

When resolving a rebase conflict, correct the files, stage them, and continue:

```bash
git add <files>
git rebase --continue
```

Use `git rebase --abort` to return to the pre-rebase state.

Rebase and commit amendment rewrite commit identities. Clean up only commits
that have not been shared, unless every affected contributor has explicitly
coordinated the rewrite. Preserve intermediate commits that explain meaningful
design or implementation steps.

## Local cleanup is not squash integration

Local cleanup makes individual feature-branch commits coherent before review.
Pull request squash merging collapses the entire branch during integration.
They are different operations: local cleanup is allowed when appropriate;
squashing commits while integrating a pull request is prohibited.

## Integrating branches

The Git guidelines require these directions:

1. Bring `main` into a feature branch using rebase or a fast-forward operation.
   Do not create a merge commit from `main` into the feature branch.
2. Integrate the feature branch into `main` with a merge commit and without
   fast-forward.

For a feature branch with its own commits, the usual update is:

```bash
git switch <feature-branch>
git rebase main
```

If the branch has no divergent commits, a fast-forward-only update is valid:

```bash
git merge --ff-only main
```

The integration into `main` must retain the branch boundary:

```bash
git switch main
git merge --no-ff <feature-branch>
```

Equivalent hosting-platform integration must select a merge commit. Do not use
squash merge, rebase merge, or an ordinary fast-forward merge for a feature
branch.

Agents may prepare or explain these operations, but must not rebase, commit,
push, or merge without explicit authorization.
