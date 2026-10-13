# Formatting and Static Analysis

This is the executable formatting procedure for Tree_nity's Go source. The
[Go conventions](../coding-style/go.md#formatting-and-imports) define the style;
this document defines how to apply and verify it.

## Procedure

Format every changed Go file explicitly:

```bash
gofmt -w <changed-go-files>
```

Do not use this step to reformat unrelated files. Then verify all Go source
under `src/`:

```bash
gofmt -l src
```

The command passes only when it prints no paths. Finally, run Go's static
analysis across all packages:

```bash
go vet ./...
```

Resolve every reported issue caused by the approved change. Report pre-existing
or out-of-scope issues without modifying them.

## Reporting

Record which files were formatted and the result of each verification command.
If the environment prevents a command from running, report the command and the
actual error instead of substituting another tool or claiming success.
