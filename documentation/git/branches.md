# Branches

## Guideline requirements

- Use a dedicated branch for every feature, bug fix, refactor, or other
  significant change.
- Give the branch a name that describes the nature of its work.
- Keep unrelated changes on separate branches.
- Keep `main` stable and suitable as the integration branch.

## Repository naming convention

Tree_nity branches use:

```text
<type>/<component>/<change>
```

Use lowercase words and hyphens. The type should normally match an applicable
[commit type](commits.md#types); the component identifies the affected area;
and the final segment describes the objective.

Examples:

```text
feat/producer/connection
fix/broker/message-persistence
refactor/protocol/parser
docs/testing/unit-tests
chore/build/makefile
```

The local `gitinette` script accepts the less specific `<type>/<name>` shape,
so a passing tool result does not by itself establish compliance with this
repository convention.

## Scope and stability

A branch represents one reviewable objective and may contain several logical
commits. Split independently reviewable work instead of using a broad shared
branch such as `feat/producer` for unrelated producer changes.

Avoid direct work on `main`, including for small changes, unless the repository
maintainers explicitly choose otherwise. This is a repository recommendation;
the guideline requirement is that significant work use a branch and that
`main` remain stable.

See [Git history](history.md#integrating-branches) for the required directions
and merge modes when updating or integrating a branch.
