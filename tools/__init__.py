"""Commercial product wrappers built on top of the sql_logic_extractor engine.

Each subpackage is one of the 4 shippable tools. Each tool exposes a CLI
entry point (cli.py) and an HTTP entry point (api.py). The actual
domain logic lives in `sql_logic_extractor.products`; these wrappers just
adapt input/output for the chosen delivery channel."""
