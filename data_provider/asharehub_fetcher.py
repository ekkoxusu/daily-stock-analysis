# -*- coding: utf-8 -*-
"""
AShareHub 数据源（当前仅用于筹码分布）

背景：
- AkShare 的 ak.stock_cyq_em 依赖东方财富公开接口，在 GitHub Actions 等海外出口上
  连续请求会被直接断连（RemoteDisconnected），批量分析时往往只有第一只股票能取到数据。
- Tushare 的 cyq_chips 属于「特色数据」，需要 5000 积分权限。
- AShareHub 提供稳定的 /v2/chips/distribution 接口，作为筹码分布的兜底源。

配置：环境变量 ASHAREHUB_API_KEY（可选 ASHAREHUB_PRIORITY / ASHAREHUB_BASE_URL）
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Optional

import pandas as pd
import requests

from .base import BaseFetcher, normalize_stock_code
from .realtime_types import ChipDistribution, safe_float

logger = logging.getLogger(__name__)


class AshareHubFetcher(BaseFetcher):
    """AShareHub 筹码分布数据源。

    priority 默认 2（越小越优先）：紧跟在 AkShare(1) 之后。AkShare 的东财筹码接口在
    海外出口上通常只有第一只股票能成功，其余会被断连，此时由本数据源接管。

    注意：本类不提供 K 线数据，若 AkShare 日线偶发失败，管理器会尝试到这里并捕获
    NotImplementedError 后继续下一个数据源，不影响行情链路。
    """

    name = "AshareHubFetcher"
    priority = int(os.getenv("ASHAREHUB_PRIORITY", "2"))
    allow_empty_daily_data = True

    capital_flow_markets = {"cn"}

    CHIP_ENDPOINT = "/v2/chips/distribution"
    FLOW_ENDPOINT = "/v2/flows/moneyflow"
    DEFAULT_BASE_URL = "https://asharehub.com"

    def __init__(self, api_key: str, priority: Optional[int] = None, timeout: int = 15) -> None:
        self._api_key = (api_key or "").strip()
        if priority is not None:
            self.priority = int(priority)
        self._timeout = timeout
        self._base_url = (os.getenv("ASHAREHUB_BASE_URL") or self.DEFAULT_BASE_URL).rstrip("/")
        self._session = requests.Session()

    def is_available(self) -> bool:
        return bool(self._api_key)

    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError("AshareHubFetcher 仅实现筹码分布，不提供 K 线数据")

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        return df

    @staticmethod
    def _to_symbol(stock_code: str) -> str:
        code = normalize_stock_code(stock_code)
        if not code:
            return ""
        if code.startswith("6"):
            return f"{code}.SH"
        if code.startswith(("0", "3")):
            return f"{code}.SZ"
        if code.startswith(("4", "8")):
            return f"{code}.BJ"
        return ""

    @staticmethod
    def _concentration(low: float, high: float) -> float:
        """与东财口径一致：(上限 - 下限) / (上限 + 下限)。"""
        if low <= 0 or high <= 0 or high <= low:
            return 0.0
        return (high - low) / (high + low)

    @staticmethod
    def _fmt_date(value: Any) -> str:
        text = str(value or "").strip()
        if len(text) == 8 and text.isdigit():
            return f"{text[:4]}-{text[4:6]}-{text[6:]}"
        return text

    def get_chip_distribution(self, stock_code: str) -> Optional[ChipDistribution]:
        if not self.is_available():
            return None

        symbol = self._to_symbol(stock_code)
        if not symbol:
            logger.debug(f"[AshareHub] 跳过非 A 股标的 {stock_code}")
            return None

        try:
            resp = self._session.get(
                f"{self._base_url}{self.CHIP_ENDPOINT}",
                params={"symbol": symbol},
                headers={"X-API-Key": self._api_key},
                timeout=self._timeout,
            )
        except Exception as exc:
            logger.warning(f"[AshareHub] 请求 {symbol} 失败: {exc}")
            return None

        if resp.status_code == 429:
            logger.warning("[AshareHub] 超出每日调用额度(429)，本日不再尝试")
            self._api_key = ""
            return None
        if resp.status_code != 200:
            logger.warning(f"[AshareHub] 筹码接口返回 {resp.status_code}: {resp.text[:120]}")
            return None

        try:
            rows = resp.json()
        except Exception as exc:
            logger.warning(f"[AshareHub] 响应解析失败: {exc}")
            return None

        if not isinstance(rows, list) or not rows:
            logger.warning(f"[AshareHub] {symbol} 筹码数据为空")
            return None

        latest = rows[0]
        for row in rows:
            if str(row.get("trade_date", "")) > str(latest.get("trade_date", "")):
                latest = row

        low90 = safe_float(latest.get("cost_5pct"))
        high90 = safe_float(latest.get("cost_95pct"))
        low70 = safe_float(latest.get("cost_15pct"))
        high70 = safe_float(latest.get("cost_85pct"))

        chip = ChipDistribution(
            code=normalize_stock_code(stock_code),
            date=self._fmt_date(latest.get("trade_date")),
            source="asharehub",
            profit_ratio=safe_float(latest.get("winner_rate")) / 100.0,
            avg_cost=safe_float(latest.get("weight_avg")),
            cost_90_low=low90,
            cost_90_high=high90,
            concentration_90=self._concentration(low90, high90),
            cost_70_low=low70,
            cost_70_high=high70,
            concentration_70=self._concentration(low70, high70),
        )

        logger.info(
            f"[筹码分布] {chip.code} 日期={chip.date}: 获利比例={chip.profit_ratio:.1%}, "
            f"平均成本={chip.avg_cost}, 90%集中度={chip.concentration_90:.2%}, "
            f"70%集中度={chip.concentration_70:.2%} (来源: AshareHub)"
        )
        return chip

    # ---------------------------------------------------------------- 资金流

    def _get_json(self, endpoint: str, params: dict) -> Optional[Any]:
        if not self.is_available():
            return None
        try:
            resp = self._session.get(
                f"{self._base_url}{endpoint}",
                params=params,
                headers={"X-API-Key": self._api_key},
                timeout=self._timeout,
            )
        except Exception as exc:
            logger.warning(f"[AshareHub] 请求 {endpoint} 失败: {exc}")
            return None
        if resp.status_code == 429:
            logger.warning("[AshareHub] 超出每日调用额度(429)，本日不再尝试")
            self._api_key = ""
            return None
        if resp.status_code != 200:
            logger.warning(f"[AshareHub] {endpoint} 返回 {resp.status_code}: {resp.text[:120]}")
            return None
        try:
            return resp.json()
        except Exception as exc:
            logger.warning(f"[AshareHub] {endpoint} 响应解析失败: {exc}")
            return None

    def get_capital_flow(self, stock_code: str, top_n: int = 5) -> dict:
        """个股资金流：主力净流入（大单+特大单）与近 5/10 日合计。

        返回结构与 AkshareFundamentalAdapter.get_capital_flow 同构，
        由 DataFetcherManager._supplement_capital_flow_from_fetchers 消费。
        """
        result: dict = {
            "status": "not_supported",
            "stock_flow": {},
            "sector_rankings": {"top": [], "bottom": []},
            "source_chain": [],
            "errors": [],
        }
        symbol = self._to_symbol(stock_code)
        if not symbol:
            return result

        today = datetime.now()
        start = (today - timedelta(days=30)).strftime("%Y%m%d")
        end = today.strftime("%Y%m%d")
        rows = self._get_json(
            self.FLOW_ENDPOINT,
            {"symbol": symbol, "start_date": start, "end_date": end},
        )
        if not isinstance(rows, list) or not rows:
            result["errors"].append("asharehub: empty moneyflow")
            return result

        rows = sorted(rows, key=lambda r: str(r.get("trade_date", "")), reverse=True)

        def _main_net_yuan(row: dict) -> Optional[float]:
            buy = safe_float(row.get("buy_lg_amount")) + safe_float(row.get("buy_elg_amount"))
            sell = safe_float(row.get("sell_lg_amount")) + safe_float(row.get("sell_elg_amount"))
            if buy == 0 and sell == 0:
                return None
            return (buy - sell) * 10000.0  # 万元 -> 元

        series = [value for value in (_main_net_yuan(row) for row in rows) if value is not None]
        if not series:
            result["errors"].append("asharehub: no main net inflow")
            return result

        def _sum(limit: int) -> Optional[float]:
            window = series[:limit]
            return round(sum(window), 2) if window else None

        result["status"] = "success"
        result["stock_flow"] = {
            "main_net_inflow": round(series[0], 2),
            "inflow_5d": _sum(5),
            "inflow_10d": _sum(10),
        }
        result["source_chain"].append("asharehub:moneyflow")
        logger.info(
            f"[资金流] {symbol} 主力净流入={series[0] / 1e8:.2f}亿 "
            f"5日={(_sum(5) or 0) / 1e8:.2f}亿 10日={(_sum(10) or 0) / 1e8:.2f}亿 (来源: AshareHub)"
        )
        return result
