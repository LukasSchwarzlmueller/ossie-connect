"""Scoping for the converters' two reporting channels."""

import logging
import warnings
from contextlib import contextmanager

CONVERTER_LOGGERS = ("ossie_microsoft", "ossie_databricks")


@contextmanager
def converting(warn: bool):
    """Run a conversion with its reporting on exactly one channel.

    Both converters report each lossy step through `warnings` *and* a logger. With no
    handler installed, the logging fallback prints every message a second time, past any
    filter on the warnings channel. A NullHandler stops that, but installing one at
    import time would reconfigure logging the importing application owns - so it is
    added here and removed again when the block exits.
    """
    added = []
    for name in CONVERTER_LOGGERS:
        logger = logging.getLogger(name)
        handler = logging.NullHandler()
        logger.addHandler(handler)
        added.append((logger, handler))
    try:
        with warnings.catch_warnings():
            if not warn:
                warnings.simplefilter("ignore")
            yield
    finally:
        for logger, handler in added:
            logger.removeHandler(handler)
