# Contributing

Thanks for your interest in the little big brain Python SDK!

## How this repository works

This repository is a **one-way export** from the private little big brain
monorepo. Canonical development, code review, and CI happen there; every commit
on `main` here is an allow-listed snapshot that already passed those gates.

Practical consequences:

- **Issues are the best way to contribute.** Bug reports with a minimal
  reproduction, API-ergonomics feedback, and documentation fixes are all
  triaged directly and usually turn around quickly.
- **Pull requests are welcome but are not merged directly.** A maintainer
  ports an accepted patch to the private repository, lands it through private
  CI, and the next export brings it back here — at which point your PR is
  closed with a reference to the canonical commit. You keep authorship credit
  in the release notes.
- Generated files (`lbb/models.py`, `contracts/openapi.json`) are produced
  from the server's Rust API types in the canonical monorepo. PRs that
  hand-edit them can't be accepted; describe the contract problem in an issue
  instead.

## Developing

Python ≥ 3.10:

```sh
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'
ruff check . && mypy lbb && pytest
```

## Releases

Maintainers release by pushing a signed `vX.Y.Z` tag matching the
`pyproject.toml` version, which publishes `lbb-sdk` via PyPI trusted
publishing after CI passes.

## Conduct & security

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) and [SECURITY.md](SECURITY.md).
Never report security issues in public GitHub issues.
