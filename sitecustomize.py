"""FuturesHunter V7.8.6 runtime bootstrap.

During Render's dependency-install phase third-party packages may not exist yet.
Skip the runtime overlay in that phase; it loads normally when the service starts.
"""
try:
    import requests  # noqa: F401
except ModuleNotFoundError:
    pass
else:
    from v786_range_overlay import *  # noqa: F401,F403
