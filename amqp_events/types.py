try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore

__all__ = ['Protocol']
