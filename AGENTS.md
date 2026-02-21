# Agents

## Before committing

Always run these checks before committing. Do not commit if either fails.

```bash
cargo fmt --check
cargo clippy -- -D warnings
```

If `cargo fmt --check` fails, run `cargo fmt` and include the formatting fix in your commit.
