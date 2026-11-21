"""Index and download datasets from the GGP data portal."""
from .client import LoginError, ggpClient

__all__ = ["ggpClient", "LoginError"]
