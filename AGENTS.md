# Agent Contribution Workflow

This file is the entry point for agents working on Tree_nity. It defines the
order of work and the rules that always apply; the detailed procedures live in
`documentation/` and are shared with human contributors.

## Sources of truth

1. **Authoritative sources** — the project subject and the Git workflow
   guidelines; they always apply.
2. **Repository conventions** — the documentation domains below; they apply
   where the authoritative sources are silent.

Documentation domains:

- [Go coding style](documentation/coding-style/README.md)
- [Testing and validation](documentation/testing/README.md)
- [Git workflow and review](documentation/git/README.md)

Treat general best practices as recommendations; never present them as
subject requirements.

## Contribution pipeline

Apply the steps below in order, scaling the depth of each step to the size and
risk of the change. Two markers control the flow:

- **(gate)** — present the step's output, then wait for the user's explicit
  answer before editing any file or continuing; never skip a gate.
- **(if selected)** — run the step only if the user selected it at the
  **Validation choice** gate; otherwise skip it.

A step ends with a reference line when a document details it.

### Prepare

1. **Understand** — read the relevant parts of the authoritative sources and
   the entry point of each documentation domain the task touches.

   Reference: [Sources of truth](#sources-of-truth).

2. **Inspect** — establish the review range to identify existing changes, then
   read the affected packages, their dependencies, their tests, and their
   current implementation.

   Reference: [review range](documentation/git/review.md#establishing-the-review-range).

3. **Plan** — define the smallest coherent change that fulfills the task,
   listing:

    - the files to create or modify;
    - the tests to add or update;
    - the target branch.

   Reference: [branch rules](documentation/git/branches.md).

4. **Plan review (gate)** — present the plan, then wait for the user to approve
   it or adjust its scope.

### Change

5. **Implement** — make the approved change, preserving the package layout,
   existing comments, and established style, and add or update the planned
   tests.

   Reference: [Go coding style](documentation/coding-style/README.md),
   [unit-test conventions](documentation/testing/unit-tests.md).

6. **Validation choice (gate)** — present a summary of the change and a
   recommended selection of **Format**, **Build**, and **Test**, then wait for
   the user to select the steps to run.

   Reference: [validation guide](documentation/testing/README.md).

### Validate

7. **Format (if selected)** — run the formatting and static-analysis
   procedure; it must report no issue.

   Reference: [formatting procedure](documentation/testing/formatting.md).

8. **Build (if selected)** — run the build procedure; it must produce `server`
   and `client`.

   Reference: [build procedure](documentation/testing/README.md#current-makefile-behavior).

9. **Test (if selected)** — run the test execution procedure; it must report
   no failure.

   Reference: [test execution procedure](documentation/testing/test-execution.md).

### Close

10. **Review** — read the complete diff, then apply the review checklist.

    Reference: [review checklist](documentation/git/review.md).

11. **Report** — summarize the work, listing:

    - the changed files and the reason for each change;
    - the commands run and their results;
    - the validation steps not selected;
    - the untested behavior and unresolved problems;
    - the reasoning behind each non-obvious design decision;
    - the proposed commit message(s).

    Reference: [Conventional Commit format](documentation/git/commits.md#format).

## Working rules

These rules apply at all times, inside or outside the contribution pipeline.

- **Ambiguity** — never guess when a source is unavailable, ambiguous, or in
  conflict with another source, the code, or the request; ask the user
  instead.

- **Explicit approval** — never perform the following actions without the
  user's explicit approval; an approval covers only the action it was given
  for:

    - creating commits, rewriting history, pushing branches, or merging pull
      requests;
    - running a command that can discard work, such as `git reset --hard`,
      `git checkout -- <path>`, `git clean`, `git push --force`, or `rm -rf`;
    - adding, removing, or upgrading an external dependency.

  Reference: [merge rules](documentation/git/merges.md).

- **Existing work** — never modify, stage, or revert changes you did not make;
  report conflicts with them to the user instead.

- **Scope** — never modify files outside the approved plan; report
  out-of-scope problems to the user instead, and return to **Plan review** if
  the scope must change.

- **Tests** — never weaken or delete existing tests to make them pass; report
  the failure to the user instead.

- **Reporting** — never claim that a command succeeded without running it, or
  silently replace a command or its result; report failures to the user
  instead, with their actual output.