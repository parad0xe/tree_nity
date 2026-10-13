# Commits

Rules for commit scope and message format.

## Requirements

### MUST

- A commit MUST contain one logical change and no unrelated modification.
- A commit message MUST follow the [message format](#format), with exceptions
  for [merge messages](merges.md#commit-message) and
  [fixup messages](history.md#fixup-messages).
- A commit message MUST describe its diff accurately.

## Format

```text
<type>[(<scope>)][!]: <description>
```

- `<type>` identifies the kind of change among the [types](#types).
- `<scope>` names the affected area.
- `!` marks a breaking change.
- `<description>` states the result in the imperative mood, in lowercase,
  without a final period.

Examples:

```text
feat(auth): add password reset
fix(parser): reject empty input
docs: describe the release process
feat(api)!: remove the legacy endpoint
```

## Types

- `feat` adds a behavior.
- `fix` corrects a defect.
- `refactor` restructures code without changing behavior.
- `perf` improves performance.
- `test` adds or changes tests.
- `docs` changes documentation only.
- `style` changes formatting only.
- `build` changes the build system or dependencies.
- `ci` changes the continuous-integration configuration.
- `chore` covers maintenance that no other type describes.
- `revert` reverts an earlier commit.
