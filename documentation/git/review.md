# Git and Code Review

Use this procedure before delivery and again before integration. Scale the
detail to the change, but do not omit a relevant check.

## 1. Establish the review range

```bash
git status --short --branch
git branch --show-current
git log --oneline --decorate main..HEAD
git diff --stat main...HEAD
git diff main...HEAD
git diff --check
```

When changes are not committed, also inspect `git diff` and the intended staged
diff separately. Confirm that the range includes all work under review.

## 2. Review branch and commits

- Does the branch follow the [branch naming convention](branches.md) and contain
  only one objective?
- Does every non-merge commit follow [Conventional Commits](commits.md)?
- Does each message accurately describe its diff?
- Is each commit coherent, with meaningful intermediate commits preserved?
- Are unrelated refactors or formatting changes absent?
- Will integration follow the [required merge strategy](history.md#integrating-branches)?

## 3. Review files and behavior

- Look for generated binaries, editor files, credentials, queues, logs, and
  other accidental artifacts.
- Check the complete diff for silent interface or externally visible behavior
  changes.
- Verify the relevant requirements in the project subject.
- Run the applicable [format, build, and test procedure](../testing/README.md).
- Check that tests actually cover the changed behavior; a package with no test
  files can still make `go test` succeed.
- Verify that documentation describes the current code and commands, labels
  requirements accurately, and uses valid relative links.

Record command failures, skipped checks, missing tests, and unresolved subject
ambiguities in the delivery report.

## Gitinette

When the `gitinette` executable is available, ordinary contributor validation
uses:

```bash
./gitinette --repo . --base-branch main
```

The current tool checks non-merge commit subjects on `main`, names of local
branches, and signs of an incorrect merge strategy. Merge-strategy findings are
manual-review warnings and do not make its exit status fail. It does not prove
message/diff coherence, subject compliance, branch scope, documentation
accuracy, compilation, or tests.

Evaluation mode adds a random sample of non-merge commits, printing each full
message and changed-file summary for an evaluator:

```bash
./gitinette --evaluation --repo . --base-branch main --sample 3
```

This sampling is evaluation-specific and requires human judgment; it is not an
extra automated correctness check. Use ordinary mode for routine validation
and evaluation mode only when preparing or performing the guideline's
evaluation procedure.

In the current working tree, `gitinette` is excluded by `.gitignore` and is not
tracked. A fresh clone therefore may not contain it. If it is unavailable,
perform the checks above manually and report the missing tool rather than
skipping Git validation.
