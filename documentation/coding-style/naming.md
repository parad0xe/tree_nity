# Go Naming

Use names that make code read naturally at the call site. These conventions
follow common Go usage and apply to new or modified declarations; unrelated
work does not justify mass renaming.

## Packages and files

- Package names are short, lowercase, singular where natural, and contain no
  underscores or mixed capitals.
- Avoid names such as `util`, `common`, or `misc` for new packages unless the
  package has a genuinely clear, cohesive role under that name.
- Go filenames are lowercase. Use underscores only where they improve an
  established multiword pattern, such as `prefix_tree`, or for Go's required
  suffixes such as `_test.go`.
- Do not repeat a package name in an exported identifier when the selector
  already provides the context: prefer `message.Parser` to
  `message.MessageParser` when `Parser` is unambiguous.

## Identifiers

- Use `MixedCaps` or `mixedCaps`, not underscores.
- Export a name only when another package needs it; capitalization is an API
  decision, not decoration.
- Keep initialisms consistently capitalized: `ID`, `URL`, `HTTP`, and therefore
  `consumerID`, not `consumerId`.
- Use short names such as `i` or `n` in small local scopes and descriptive names
  when values live longer or carry domain meaning.
- Receiver names should be short, related to the type, and consistent across
  its methods. Do not use `this` or `self`.
- Do not prefix getters with `Get`; use `State()` rather than `GetState()` when
  the shorter name is clear. A setter may use `SetState`.

## Functions, types, and interfaces

- Name functions and methods for the action or result they provide. Avoid
  repeating context already supplied by the package or receiver.
- Name types with nouns that describe the represented concept.
- Use the `-er` form for a single-method interface only when it describes the
  behavior naturally, such as `Reader`; avoid forced or vague interface names.
- Prefer defining a small interface near the code that consumes it rather than
  next to a concrete implementation without a demonstrated need.

Keep domain terminology consistent across producers, consumers, topics,
messages, payloads, and prefix matching. Correct spelling in new APIs; changing
an existing misspelled identifier may be a compatibility change and must be
handled explicitly.
