"""V7.9.1 missed-trade auditor bridge for ordinary paper-backed Core candidates.

The main v791 overlay already audits the live-only bridge. Normal Core ENTRY
candidates call create_paper_trade(), which also evaluates the live selector but
uses a separate path. Wrap that path after v791 is armed so every selector SKIP
gets the same durable counterfactual TP/SL tracking.

No execution/selection logic is changed here.
"""
import sys
import threading
import time

import live_executor_v70 as live
import v791_challenger_rotation as core

_PATCHED = False
_LOCK = threading.RLock()


def _patch(main):
    global _PATCHED
    with _LOCK:
        if _PATCHED:
            return True
        if not getattr(core, "_PATCHED", False):
            return False
        if not hasattr(main, "create_paper_trade"):
            return False

        original_create = main.create_paper_trade

        def create_paper_wrapper(result, trades):
            trade = original_create(result, trades)
            try:
                gate = (result or {}).get("v70_live_gate") or {}
                if not gate.get("eligible"):
                    source_key = (trade or {}).get("source_key")
                    if not source_key and hasattr(main, "_v68_signal_key"):
                        source_key = main._v68_signal_key(result)
                    if source_key:
                        core._arm_missed_candidate(
                            main,
                            result,
                            source_key,
                            {"executed": False},
                        )
            except Exception as exc:
                live._diag(
                    f"V7.9.1 MISSED AUDIT paper-backed wrapper error: "
                    f"{type(exc).__name__}: {exc}"
                )
            return trade

        main.create_paper_trade = create_paper_wrapper
        _PATCHED = True
        live._diag("V7.9.1 missed-audit paper-backed bridge armed; all Core SKIP paths covered")
        return True


def _bootstrap():
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            main = sys.modules.get("__main__")
            if main is not None and _patch(main):
                return
        except Exception as exc:
            live._diag(
                f"V7.9.1 missed-audit paper bridge retry: {type(exc).__name__}: {exc}"
            )
        time.sleep(0.25)
    live._diag("V7.9.1 missed-audit paper bridge gave up after 300s")


threading.Thread(target=_bootstrap, name="V791PaperAuditBridge", daemon=True).start()
