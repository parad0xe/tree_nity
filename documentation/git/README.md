# Git Workflow

This domain is the single source of truth for Tree_nity's Git workflow. The
mandatory rules come from `en.git_guidelines.pdf`; repository conventions add
specific naming and review practices without weakening those rules.

## Rule levels

- **Guideline requirement** means the Git guidelines require it.
- **Repository convention** means Tree_nity has adopted it consistently where
  the guidelines leave a choice.
- **Recommendation** is practical guidance and is not presented as mandatory.

## Documents

- [Branches](branches.md) — branch scope, stability, and naming.
- [Commits](commits.md) — Conventional Commits, accuracy, and granularity.
- [History](history.md) — cleanup, rebasing, and mandatory merge strategies.
- [Review](review.md) — the contributor and reviewer validation procedure.

## Core workflow

Significant work is developed on a focused branch, recorded as coherent
Conventional Commits, kept current with `main` by rebase or fast-forward, and
integrated into `main` with a non-fast-forward merge commit. Squash integration
is prohibited because meaningful intermediate commits are part of the project
history.

The `gitinette` tool can identify some mechanical violations, but it does not
replace review of the diff, subject compliance, build results, tests, or the
merge strategy. See the [review procedure](review.md#gitinette).
