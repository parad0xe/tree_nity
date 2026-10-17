# Project Profile

Project-specific settings used by the reusable modules and by agents. Edit this
file to reuse the documentation in another project.

## Settings

| Setting            | Value                |
|--------------------|----------------------|
| Project name       | Tree_nity            |
| Description        | Message queue system |
| Integration branch | `main`               |
| Git remote         | `origin`             |
| Required CI checks | `Test / Unit tests`  |

## Sources of truth

- The project subject: `subject.pdf`.
- The Git workflow guidelines: `git_guidelines.pdf`.

## Commands

| Step            | Command                                                                        |
|-----------------|--------------------------------------------------------------------------------|
| Format          | `gofmt -w <changed-files>`                                                     |
| Format check    | `test -z "$(gofmt -l .)"`  |
| Static analysis | `go vet ./...`                                                                 |
| Build           | `make`                                                                         |
| Test            | `make test`                                                                    |
| Race detection  | `go test -race ./...`                                                          |

## Project rules

Rules that complement or override the modules.

### MUST

- A change affecting the command-line interface, standard output, or IPC MUST
  pass the subject's test scenario.
