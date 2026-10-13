# Commits

## Guideline requirements

- Use Conventional Commits.
- Make each commit one coherent logical change.
- Make the message accurately describe the resulting diff.
- Preserve intermediate commits when they communicate meaningful development
  history.

## Format

```text
<type>(<optional-scope>)<optional-!>: <description>
```

Examples:

```text
feat(producer): implement broker connection
fix(protocol): reject invalid message length
test(prefix-tree): cover empty prefixes
docs(git): document review procedure
build: correct server package path
```

The description should state the result, not an editing activity. Avoid vague
messages such as `update`, `changes`, `work`, or `fix`.

## Types

Use the narrowest accurate type. The repository's `gitinette` validator
recognizes:

- `feat` — new behavior;
- `fix` — a defect correction;
- `refactor` — internal restructuring with no intended behavior change;
- `test` — test additions or changes;
- `docs` — documentation-only changes;
- `style` — formatting or style with no behavior change;
- `perf` — a performance improvement;
- `build` — build system or dependency changes;
- `ci` — continuous-integration configuration;
- `chore` — maintenance not covered above;
- `revert` — reversal of an earlier commit.

The optional scope names the affected area, for example `producer`, `consumer`,
`protocol`, `prefix-tree`, `build`, or `git`.

## Granularity and coherence

Use one commit for one reason to change the repository. A commit is too broad
when it mixes unrelated behavior, cleanup, and documentation. It is too narrow
when it records an arbitrary editing step with no useful purpose in review or
history.

Line count does not determine granularity. Do not squash a meaningful sequence
merely to reduce the number of commits. Temporary corrections can instead use
fixup commits and local cleanup as described in [Git history](history.md).

## Breaking changes

Mark an intentionally incompatible interface or behavior change with `!`:

```text
feat(protocol)!: change message header format
```

or with a `BREAKING CHANGE:` footer that explains the migration. A commit
marker does not replace the requirement to make such a change explicit during
implementation and review.
