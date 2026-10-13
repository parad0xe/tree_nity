# Agent Contribution Workflow

Rules and workflow for agents contributing to this repository. Project-specific
settings live in the [project profile](documentation/project/README.md).

## Keywords

- **MUST** and **MUST NOT** mark a requirement.
- **SHOULD** and **SHOULD NOT** mark a recommendation; a deviation MUST be
  justified.

## Requirements

### MUST

- An agent MUST follow the [workflow](#workflow) for every change.
- An agent MUST stop at each gate and wait for the user's explicit answer
  before editing any file or continuing.
- An agent MUST apply the
  [sources of truth](documentation/project/README.md#sources-of-truth) first,
  then the [modules](documentation/project/README.md#modules) where those
  sources are silent.
- An agent MUST ask the user when a source is unavailable, ambiguous, or in
  conflict with another source, the code, or the request.
- An agent MUST return to the plan gate when the scope must change.
- An agent MUST preserve the existing layout, comments, and style.
- An agent MUST report every failure with its actual output.

### MUST NOT

- An agent MUST NOT present a recommendation as a requirement.
- An agent MUST NOT modify files outside the approved plan.
- An agent MUST NOT modify, stage, or revert changes it did not make.
- An agent MUST NOT weaken or delete existing tests to make them pass.
- An agent MUST NOT claim that a command succeeded without running it.
- An agent MUST NOT silently replace a command or its result.
- An agent MUST NOT create commits, rewrite history, push branches, or merge
  pull requests without explicit approval.
- An agent MUST NOT run a command that can discard work, such as
  `git reset --hard`, `git checkout -- <path>`, `git clean`,
  `git push --force`, or `rm -rf`, without explicit approval.
- An agent MUST NOT add, remove, or upgrade a dependency without explicit
  approval.
- An approval MUST NOT extend beyond the action it was given for.

## Recommendations

### SHOULD

- An agent SHOULD scale the depth of each step to the size and risk of the
  change.
- An agent SHOULD report out-of-scope problems it notices.

## Workflow

### Prepare

1. Read the [project profile](documentation/project/README.md), then the
   relevant parts of its sources of truth and modules.
2. Establish the
   [review range](documentation/git/review.md#establishing-the-review-range),
   then read the affected code, its dependencies, and its tests.
3. Define the smallest coherent change, listing:
   - the files to create or modify;
   - the tests to add or update;
   - the target [branch](documentation/git/branches.md).
4. **Gate:** present the plan, then wait for the user to approve or adjust it.

### Change

5. Implement the approved change following the
   [coding style](documentation/coding-style/README.md) and the
   [unit-test conventions](documentation/testing/unit-tests.md).
6. **Gate:** present a summary of the change and a recommended selection among
   formatting, build, and tests, then wait for the user to select the steps to
   run.

### Validate (selected steps only)

7. Run the formatting and static-analysis
   [commands](documentation/project/README.md#commands).
8. Run the build [command](documentation/project/README.md#commands).
9. Run the test [commands](documentation/project/README.md#commands).

### Close

10. Apply the [review rules](documentation/git/review.md) to the complete diff.
11. Report the work, listing:
    - the changed files and the reason for each change;
    - the commands run and their results;
    - the validation steps not selected;
    - the untested behavior and unresolved problems;
    - the reasoning behind each non-obvious design decision;
    - the proposed [commit messages](documentation/git/commits.md#format).
