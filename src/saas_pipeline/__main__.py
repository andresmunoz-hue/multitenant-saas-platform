"""Package entry: ``python -m saas_pipeline`` → CLI."""

from saas_pipeline.interfaces.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
