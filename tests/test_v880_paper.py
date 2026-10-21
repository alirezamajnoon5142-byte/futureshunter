"""Non-execution smoke tests for the independent V8.8 paper-only research module."""
import importlib.util
import os
from pathlib import Path
import sys
import time
import types
import unittest
from unittest.mock import Mock
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]

class V880PaperTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["V880_PAPER_ENABLED"]="false"
        sys.modules["live_executor_v70"]=types.SimpleNamespace(_db=lambda *a,**k:None,_diag=lambda *a,**k:None)
        spec=importlib.util.spec_from_file_location("fh_test_v880",ROOT/"v880_tomcamp_paper.py")
        cls.mod=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.mod)

    def test_universe_and_four_targets(self):
        m=self.mod
        self.assertEqual(len(m.SYMBOLS),len(set(m.SYMBOLS)))
        self.assertEqual(m.TARGETS,(.75,1.,1.5,2.))
        for sym in ("NAS100_USDT","SP500_USDT","XAU_USDT","ONDO_USDT",
                    "BTC_USDT","ETH_USDT","SOL_USDT","AAVE_USDT","BNB_USDT"):
            self.assertIn(sym,m.SYMBOLS)
        self.assertTrue(all(sym.endswith("_USDT") for sym in m.SYMBOLS))

    def test_only_hour4_and_closed_candles(self):
        m=self.mod;end=(int(time.time())//m.BAR_SECONDS)*m.BAR_SECONDS
        rows=[]
        for i in range(68):
            ts=end-(67-i)*m.BAR_SECONDS
            rows.append(dict(time=ts,open=100,high=101,low=99,close=100))
        class Main:
            def __init__(self):self.calls=[]
            def get_candles(self,sym,iv):
                self.calls.append((sym,iv))
                return pd.DataFrame(rows)
        main=Main();d=m.candles(main,"ONDO_USDT")
        self.assertEqual(main.calls,[("ONDO_USDT","Hour4")])
        self.assertIsNotNone(d)
        # The candle that starts at current 4H boundary is forming.
        self.assertLess(float(d.time.iloc[-1]),end)

    def test_stats_show_every_instrument_without_data(self):
        m=self.mod;original=m.db
        try:
            m.db=lambda *a,**k: []
            summary=m.stats(detail=False)
            self.assertIn("\n",summary)
            self.assertIn("NASDAQ",summary)
            self.assertIn("ES proxy",summary)
            self.assertIn("XAU",summary)
            self.assertIn("ONDO",summary)
            self.assertIn("/tomcamp",summary)
            detail=m.stats(detail=True)
            self.assertIn("TP variants per market",detail)
            self.assertIn(".75R",detail)
            self.assertIn("2R",detail)
        finally:m.db=original

    def test_scan_does_not_backfill_or_place_orders(self):
        m=self.mod
        # A stale/old last closed candle must not create a new paper row.
        old_ts=(int(time.time())//m.BAR_SECONDS)*m.BAR_SECONDS-100*m.BAR_SECONDS
        rows=[dict(time=old_ts-(65-i)*m.BAR_SECONDS,open=100,
                   high=101,low=99,close=100) for i in range(66)]
        m_health=m.HEALTH.copy()
        old_candles,old_db,old_candidate=m.candles,m.db,m.candidate
        statements=[]
        try:
            m.candles=lambda main,sym:pd.DataFrame(rows)
            m.db=lambda sql,args=(),mode=None:(statements.append(sql) or [])
            m.candidate=lambda d:("LONG",100.,99.,1.,False,2.,old_ts)
            m.scan(types.SimpleNamespace())
            self.assertFalse(any("INSERT INTO fh_v880_signals" in x for x in statements))
        finally:
            m.candles,m.db,m.candidate=old_candles,old_db,old_candidate
            m.HEALTH.clear();m.HEALTH.update(m_health)

    def test_ict_bootstrap_removed_without_deleting_history(self):
        text=(ROOT/"sitecustomize.py").read_text(encoding="utf-8")
        self.assertNotIn("    import v840_ict_dol_shadow  # noqa",text)
        self.assertTrue((ROOT/"v840_ict_dol_shadow.py").exists())


    def test_bullish_structure_sweep_engulfing_candidate(self):
        import math
        m=self.mod
        bars=[]
        for i in range(78):
            close=100+.15*i+3*math.sin(i*2*math.pi/10)
            bars.append(dict(time=i*14400.,open=close-.2,high=close+.7,
                             low=close-.7,close=close))
        base=pd.DataFrame(bars)
        pclose=float(base.close.iloc[-1])+1
        p=dict(time=78*14400.,open=pclose+1,high=pclose+1.6,
               low=float(base.low.min())-5,close=pclose)
        c=dict(time=79*14400.,open=pclose-.5,high=p["high"]+1.5,
               low=pclose-1,close=p["high"]+1)
        d=pd.concat([base,pd.DataFrame([p,c])],ignore_index=True)
        result=m.candidate(d)
        self.assertIsNotNone(result)
        self.assertEqual(result[0],"LONG")
        self.assertLess(result[2],result[1])
        self.assertEqual(result[-1],c["time"])

    def test_ambiguous_ohlc_bar_settles_stop_first(self):
        m=self.mod
        original=(m.candles,m.db,m.candidate)
        old_time=(int(time.time())//m.BAR_SECONDS)*m.BAR_SECONDS-20*m.BAR_SECONDS
        d=pd.DataFrame([dict(time=old_time,open=100.,high=100.5,low=99.5,close=100.),
                        dict(time=old_time+m.BAR_SECONDS,open=100.,high=103.,low=97.,close=101.)])
        seen=[]
        def fake_db(sql,args=(),mode=None):
            if "SELECT s.key,s.bar_ts" in sql:
                return [("BTC-test",old_time,"LONG",100.,99.,tp) for tp in m.TARGETS]
            seen.append((sql,args))
            return None
        try:
            m.candles=lambda main,sym:d if sym=="BTC_USDT" else None
            m.db=fake_db
            m.candidate=lambda _:None
            m.scan(types.SimpleNamespace())
            settled=[args for sql,args in seen if "UPDATE fh_v880_exits" in sql]
            self.assertEqual(len(settled),4)
            self.assertTrue(all(float(x[0])<-1.0 for x in settled))
            self.assertEqual({float(x[3]) for x in settled},set(m.TARGETS))
        finally:
            m.candles,m.db,m.candidate=original

if __name__=="__main__":unittest.main()
