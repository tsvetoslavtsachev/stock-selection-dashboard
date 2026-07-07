"""Research namespace for the Stock Selection Dashboard.

Standing research tooling that lives ALONGSIDE (never inside) the production
pipeline. Nothing here writes to ``src/``, ``site/``, ``data/``, or to the
canonical price-archive / data-core / collectors — those are strictly read-only
inputs. Outputs go to ``research/results/``.
"""
