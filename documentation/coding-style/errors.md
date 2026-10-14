# Errors

Rules for handling errors.

## Requirements

### MUST NOT

- An error MUST NOT be ignored without a comment explaining why.
- A recoverable runtime failure MUST NOT terminate the program.

## Handling failures

A failure that prevents startup or continued operation may cause a controlled
exit after a clear diagnostic and any necessary cleanup.
