# Project Profile

Project-specific settings used by the reusable modules and by agents. Edit this
file to reuse the documentation in another project.

## Settings

| Setting            | Value                |
|--------------------|----------------------|
| Project name       | Tree_nity            |
| Description        | Message queue system |
| Integration branch | `main`               |
| Required CI checks | `Test / Unit tests`  |

## Sources of truth

These sources prevail over every module.

- The project subject: `en.subject.treenity.pdf`.
- The Git workflow guidelines: `en.git_guidelines.pdf`.

## Modules

- [Git](../git/README.md)
- [Coding style](../coding-style/README.md), with its
  [Go rules](../coding-style/go/README.md)
- [Testing](../testing/README.md), with its [Go rules](../testing/go/README.md)

## Commands

| Step            | Command                    |
|-----------------|----------------------------|
| Format          | `gofmt -w <changed-files>` |
| Format check    | `test -z "$(gofmt -l .)"`  |
| Static analysis | `go vet ./...`             |
| Build           | `make`                     |
| Test            | `make test`                |
| Race detection  | `go test -race ./...`      |

Each command MUST exit with code 0.

## Project rules

Rules that complement or override the modules.

### MUST

- A change affecting the command-line interface, standard output, or IPC MUST
  pass the subject's test scenario.
