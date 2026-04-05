"""
Shared rate limiter singleton.

Putting the limiter here (instead of main.py) breaks the circular import
that would otherwise occur when route modules try to import from main.py
while main.py imports those same route modules.
"""

from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
