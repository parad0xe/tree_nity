# Coding Style

This domain defines the coding conventions for Tree_nity. It is intended for
both contributors and agents.

## Rule sources

- The project subject determines the required language and behavior.
- [Go conventions](go.md) are repository conventions based on standard Go
  practice and the existing codebase; they are not presented as extra subject
  requirements.
- [Naming conventions](naming.md) define the shared vocabulary for Go code and
  files.

Prefer standard Go idioms over project-specific rules. Existing package
boundaries and package names must be preserved during an unrelated change even
when a different structure might also be reasonable. Architecture changes
require their own explicit objective and review.
