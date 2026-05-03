#!/usr/bin/env python3
"""Fetch quarter-hourly or daily CSV exports from the LinzNetz consumption portal."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html import unescape
from pathlib import Path
from typing import Literal

import httpx

logger = logging.getLogger(__name__)

CONSUMPTION_URL = (
    "https://services.linznetz.at/verbrauchsdateninformation/consumption.jsf"
)
NAV_PARAM = "/de/linz_netz_website/online_services/serviceportal/meine_verbraeuche/verbrauchsdaten"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

Granularity = Literal["quarter", "day"]
Unit = Literal["KWH", "EUR"]


class FetchError(RuntimeError):
    pass


class NoDataError(FetchError):
    """Raised when the portal has no data for the requested range."""


@dataclass
class FormState:
    view_state: str
    granularity_field: str
    granularity_radio_indices: dict[str, str]
    plant_field: str | None
    plant_id: str | None
    from_date_source: str | None
    to_date_source: str | None
    unit_field: str | None


def _extract(pattern: str, text: str, label: str, *, group: int = 1) -> str:
    m = re.search(pattern, text)
    if not m:
        raise FetchError(f"could not find {label} in response")
    return unescape(m.group(group))


def _extract_view_state(html: str) -> str:
    return _extract(
        r'name="jakarta\.faces\.ViewState"[^>]*value="([^"]+)"', html, "ViewState"
    )


def _extract_view_state_from_partial(xml: str) -> str:
    # PrimeFaces partial-response: <update id="...:javax.faces.ViewState..."><![CDATA[value]]></update>
    m = re.search(
        r'<update[^>]*id="[^"]*ViewState[^"]*"[^>]*>'
        r"(?:<!\[CDATA\[)?([^<\]]+)(?:\]\]>)?</update>",
        xml,
    )
    if m:
        return m.group(1).strip()
    # Fallback: <input ... name="jakarta.faces.ViewState" value="...">
    return _extract_view_state(xml)


def _granularity_value(g: Granularity) -> str:
    return {"quarter": "ConsumQuarter", "day": "ConsumDaily"}[g]


def _fmt_de(d: date) -> str:
    return d.strftime("%d.%m.%Y")


def _replace_view_state(state: "FormState", view_state: str) -> "FormState":
    return FormState(
        view_state=view_state,
        granularity_field=state.granularity_field,
        granularity_radio_indices=state.granularity_radio_indices,
        plant_field=state.plant_field,
        plant_id=state.plant_id,
        from_date_source=state.from_date_source,
        to_date_source=state.to_date_source,
        unit_field=state.unit_field,
    )


class LinzNetzFetcher:
    def __init__(self, username: str, password: str, *, timeout: float = 30.0):
        self.username = username
        self.password = password
        self._client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout,
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
            },
        )

    async def __aenter__(self) -> "LinzNetzFetcher":
        return self

    async def __aexit__(self, *exc) -> None:
        await self._client.aclose()

    async def _login_and_get_consumption(self) -> tuple[str, str]:
        params = {"nav": NAV_PARAM}
        r = await self._client.get(CONSUMPTION_URL, params=params)
        r.raise_for_status()

        if "login-actions/authenticate" in r.text:
            action = _extract(
                r'<form[^>]*action="([^"]*login-actions/authenticate[^"]*)"',
                r.text,
                "kc form action",
            )
            login = await self._client.post(
                action,
                data={"username": self.username, "password": self.password},
            )
            login.raise_for_status()
            r = login

        if "consumption.jsf" not in str(r.url):
            raise FetchError(f"unexpected landing page after login: {r.url}")
        if 'name="myForm1"' not in r.text:
            raise FetchError("consumption form not found — login likely failed")
        return r.text, str(r.url)

    @staticmethod
    def _parse_initial_state(html: str) -> FormState:
        view_state = _extract_view_state(html)
        granularity_field = _extract(
            r'name="([^"]*:grid_eval:selectedClass)"', html, "granularity field"
        )
        radio_pattern = (
            r'name="' + re.escape(granularity_field) + r'"[^>]*'
            r'id="[^"]*:(\d+)"[^>]*value="([^"]*)"'
        )
        indices: dict[str, str] = {}
        for m in re.finditer(radio_pattern, html):
            indices[m.group(2)] = m.group(1)
        if not indices:
            raise FetchError("could not parse granularity radio indices")

        plant_field = None
        plant_id = None
        pm = re.search(r'name="([^"]*:selectedPlantID)"[^>]*value="([^"]*)"', html)
        if pm:
            plant_field, plant_id = pm.group(1), pm.group(2)

        from_src = re.search(
            r'<script id="([^"]+)"[^>]*>changeFromDate = function', html
        )
        to_src = re.search(r'<script id="([^"]+)"[^>]*>assignToDate = function', html)

        return FormState(
            view_state=view_state,
            granularity_field=granularity_field,
            granularity_radio_indices=indices,
            plant_field=plant_field,
            plant_id=plant_id,
            from_date_source=from_src.group(1) if from_src else None,
            to_date_source=to_src.group(1) if to_src else None,
            unit_field=None,
        )

    @staticmethod
    def _find_unit_field(html: str, granularity_field: str) -> str | None:
        for m in re.finditer(
            r'name="([^"]*:selectedClass)"[^>]*value="(KWH|EUR)"', html
        ):
            name = m.group(1)
            if name != granularity_field:
                return name
        return None

    async def _ajax_post(self, data: dict[str, str]) -> httpx.Response:
        headers = {
            "Faces-Request": "partial/ajax",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/xml, text/xml, */*; q=0.01",
            "Origin": "https://services.linznetz.at",
            "Referer": f"{CONSUMPTION_URL}?nav={NAV_PARAM}",
        }
        r = await self._client.post(CONSUMPTION_URL, data=data, headers=headers)
        r.raise_for_status()
        return r

    async def _select_granularity(
        self,
        state: FormState,
        granularity: Granularity,
        date_from: date,
        date_to: date,
    ) -> FormState:
        value = _granularity_value(granularity)
        idx = state.granularity_radio_indices.get(value)
        if idx is None:
            raise FetchError(
                f"granularity {value} not in radios: {state.granularity_radio_indices}"
            )
        source = f"{state.granularity_field}:{idx}"
        data = {
            "jakarta.faces.partial.ajax": "true",
            "jakarta.faces.source": source,
            "jakarta.faces.partial.execute": state.granularity_field,
            "jakarta.faces.partial.render": "myForm1",
            "jakarta.faces.behavior.event": "change",
            "jakarta.faces.partial.event": "change",
            "myForm1": "myForm1",
            state.granularity_field: value,
            "myForm1:calendarFromRegion": _fmt_de(date_from),
            "myForm1:calendarToRegion": _fmt_de(date_to),
            "myForm1:periodRange": "valid",
            "jakarta.faces.ViewState": state.view_state,
        }
        r = await self._ajax_post(data)
        inner_match = re.search(
            r'<update id="myForm1"[^>]*>\s*<!\[CDATA\[(.*?)\]\]>\s*</update>',
            r.text,
            re.DOTALL,
        )
        if not inner_match:
            raise FetchError(
                "granularity-change response did not contain myForm1 update"
            )
        inner_html = inner_match.group(1)
        return FormState(
            view_state=_extract_view_state_from_partial(r.text),
            granularity_field=state.granularity_field,
            granularity_radio_indices=state.granularity_radio_indices,
            plant_field=state.plant_field,
            plant_id=state.plant_id,
            from_date_source=state.from_date_source,
            to_date_source=state.to_date_source,
            unit_field=self._find_unit_field(inner_html, state.granularity_field),
        )

    async def _set_calendar(
        self, state: FormState, source: str, param_name: str, value: str
    ) -> str:
        if not state.plant_field or not state.plant_id:
            raise FetchError("plant field/id missing — cannot send calendar AJAX")
        data = {
            "jakarta.faces.partial.ajax": "true",
            "jakarta.faces.source": source,
            "jakarta.faces.partial.execute": "@all",
            "jakarta.faces.partial.render": "myForm1:panel_calendarToRegion",
            source: source,
            param_name: value,
            "myform": "myform",
            state.plant_field: state.plant_id,
            "plantSelection": state.plant_id,
            "jakarta.faces.ViewState": state.view_state,
        }
        r = await self._ajax_post(data)
        return _extract_view_state_from_partial(r.text)

    async def _set_dates(
        self, state: FormState, date_from: date, date_to: date
    ) -> FormState:
        if not state.from_date_source or not state.to_date_source:
            raise FetchError("calendar widget script ids missing")
        from_src = state.from_date_source
        to_src = state.to_date_source
        vs = await self._set_calendar(
            state, from_src, "changeFromDate", _fmt_de(date_from)
        )
        state2 = _replace_view_state(state, vs)
        vs = await self._set_calendar(state2, to_src, "assignToDate", _fmt_de(date_to))
        return _replace_view_state(state2, vs)

    @staticmethod
    def _find_csv_button(xml: str) -> str:
        pattern = (
            r'<a[^>]*id="(myForm1:exportAreaID:[^"]+)"[^>]*>'
            r"(?:(?!</a>).)*?CSV-Datei exportieren"
        )
        m = re.search(pattern, xml, re.DOTALL)
        if not m:
            raise FetchError("CSV export button not found in display response")
        return m.group(1)

    async def _click_display(
        self,
        state: FormState,
        date_from: date,
        date_to: date,
        granularity: Granularity,
        unit: Unit,
    ) -> tuple[str, str]:
        data = {
            "jakarta.faces.partial.ajax": "true",
            "jakarta.faces.source": "myForm1:btnIdA1",
            "jakarta.faces.partial.execute": "myForm1:btnIdA1",
            "jakarta.faces.partial.render": "myForm1:list",
            "jakarta.faces.behavior.event": "action",
            "jakarta.faces.partial.event": "click",
            "myForm1": "myForm1",
            state.granularity_field: _granularity_value(granularity),
            "myForm1:calendarFromRegion": _fmt_de(date_from),
            "myForm1:calendarToRegion": _fmt_de(date_to),
            "myForm1:periodRange": "valid",
            "jakarta.faces.ViewState": state.view_state,
        }
        if state.unit_field:
            data[state.unit_field] = unit
        r = await self._ajax_post(data)
        if "exportAreaID" not in r.text:
            raise NoDataError(
                f"no data available for {_fmt_de(date_from)}–{_fmt_de(date_to)} "
                f"(granularity={granularity})"
            )
        csv_button = self._find_csv_button(r.text)
        return _extract_view_state_from_partial(r.text), csv_button

    async def _download_csv(
        self,
        state: FormState,
        view_state: str,
        csv_button: str,
        date_from: date,
        date_to: date,
        granularity: Granularity,
        unit: Unit,
    ) -> tuple[bytes, str]:
        data = {
            "myForm1": "myForm1",
            state.granularity_field: _granularity_value(granularity),
            "myForm1:calendarFromRegion": _fmt_de(date_from),
            "myForm1:calendarToRegion": _fmt_de(date_to),
            "myForm1:periodRange": "valid",
            csv_button: csv_button,
            "jakarta.faces.ViewState": view_state,
        }
        if state.unit_field:
            data[state.unit_field] = unit
        headers = {
            "Origin": "https://services.linznetz.at",
            "Referer": f"{CONSUMPTION_URL}?nav={NAV_PARAM}",
            "Accept": "text/csv,application/octet-stream,*/*;q=0.8",
        }
        r = await self._client.post(CONSUMPTION_URL, data=data, headers=headers)
        r.raise_for_status()

        ctype = r.headers.get("content-type", "")
        if "html" in ctype.lower():
            raise FetchError(
                f"expected CSV, got HTML (ctype={ctype}) — session may have expired"
            )

        filename = "consumption.csv"
        cd = r.headers.get("content-disposition", "")
        m = re.search(r'filename="?([^";]+)"?', cd)
        if m:
            filename = m.group(1)
        return r.content, filename

    async def fetch(
        self,
        date_from: date,
        date_to: date,
        *,
        granularity: Granularity = "quarter",
        unit: Unit = "KWH",
    ) -> tuple[bytes, str]:
        if date_to < date_from:
            raise ValueError("date_to must be >= date_from")
        html, _ = await self._login_and_get_consumption()
        state = self._parse_initial_state(html)
        logger.info("authenticated; plant %s", state.plant_id)
        state = await self._select_granularity(state, granularity, date_from, date_to)
        logger.info("granularity=%s, unit field=%s", granularity, state.unit_field)
        state = await self._set_dates(state, date_from, date_to)
        logger.info("calendar set: %s → %s", _fmt_de(date_from), _fmt_de(date_to))
        new_view_state, csv_button = await self._click_display(
            state, date_from, date_to, granularity, unit
        )
        logger.info("table rendered, CSV button=%s", csv_button)
        return await self._download_csv(
            state, new_view_state, csv_button, date_from, date_to, granularity, unit
        )


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


async def _amain(args: argparse.Namespace) -> int:
    user = args.username or os.environ.get("LINZNETZ_USERNAME")
    pwd = args.password or os.environ.get("LINZNETZ_PASSWORD")
    if not user or not pwd:
        print(
            "error: credentials missing (use --username/--password or LINZNETZ_USERNAME/LINZNETZ_PASSWORD)",
            file=sys.stderr,
        )
        return 2

    today = date.today()
    date_from = _parse_date(args.date_from) if args.date_from else today.replace(day=1)
    date_to = _parse_date(args.date_to) if args.date_to else today - timedelta(days=1)

    out = (
        Path(args.output)
        if args.output
        else Path(f"linznetz_{date_from}_{date_to}.csv")
    )

    try:
        async with LinzNetzFetcher(user, pwd) as f:
            body, server_name = await f.fetch(
                date_from, date_to, granularity=args.granularity, unit="KWH"
            )
    except NoDataError as e:
        print(f"no data: {e}", file=sys.stderr)
        return 3
    except FetchError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    out.write_bytes(body)
    print(f"saved {len(body)}B to {out} (server filename: {server_name})")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument(
        "--date-from", help="YYYY-MM-DD; defaults to first of current month"
    )
    parser.add_argument("--date-to", help="YYYY-MM-DD; defaults to yesterday")
    parser.add_argument("--granularity", choices=["quarter", "day"], default="quarter")
    parser.add_argument(
        "--output", "-o", help="output path; default: linznetz_<from>_<to>.csv"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    sys.exit(main())
