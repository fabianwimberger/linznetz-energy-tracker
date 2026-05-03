"""Tests for linznetz_fetcher module."""

import argparse
import asyncio
import sys
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from linznetz_fetcher import (
    FetchError,
    FormState,
    LinzNetzFetcher,
    NoDataError,
    _amain,
    _extract,
    _extract_view_state,
    _extract_view_state_from_partial,
    _fmt_de,
    _granularity_value,
    _parse_date,
    _replace_view_state,
    main,
)


class TestExtract:
    def test_found(self):
        assert _extract(r"id=\"(\d+)\"", '<div id="42">', "id") == "42"

    def test_not_found(self):
        with pytest.raises(FetchError, match="could not find foo"):
            _extract(r"x", "y", "foo")

    def test_unescape(self):
        assert _extract(r"(.&quot;.)", "a&quot;b", "q") == 'a"b'


class TestExtractViewState:
    def test_input(self):
        html = '<input name="jakarta.faces.ViewState" value="abc123">'
        assert _extract_view_state(html) == "abc123"

    def test_missing(self):
        with pytest.raises(FetchError):
            _extract_view_state("")


class TestExtractViewStateFromPartial:
    def test_cdata(self):
        xml = '<update id="j_id1:javax.faces.ViewState:0"><![CDATA[vs1]]></update>'
        assert _extract_view_state_from_partial(xml) == "vs1"

    def test_no_cdata(self):
        xml = '<update id="j_id1:javax.faces.ViewState:0">vs2</update>'
        assert _extract_view_state_from_partial(xml) == "vs2"

    def test_fallback(self):
        xml = '<input name="jakarta.faces.ViewState" value="vs3">'
        assert _extract_view_state_from_partial(xml) == "vs3"


class TestGranularityValue:
    def test_quarter(self):
        assert _granularity_value("quarter") == "ConsumQuarter"

    def test_day(self):
        assert _granularity_value("day") == "ConsumDaily"


class TestFmtDe:
    def test_date(self):
        assert _fmt_de(date(2024, 1, 5)) == "05.01.2024"


class TestReplaceViewState:
    def test_replaces(self):
        old = FormState(
            view_state="old",
            granularity_field="gf",
            granularity_radio_indices={},
            plant_field="pf",
            plant_id="pid",
            from_date_source="fs",
            to_date_source="ts",
            unit_field="uf",
        )
        new = _replace_view_state(old, "new")
        assert new.view_state == "new"
        assert new.granularity_field == "gf"


class TestParseInitialState:
    def _html(self, extras=""):
        return (
            '<input name="jakarta.faces.ViewState" value="vs1">'
            '<input name="myForm:grid_eval:selectedClass" value="x">'
            '<input name="myForm:grid_eval:selectedClass" id="myForm:0" value="ConsumQuarter">'
            '<input name="myForm:grid_eval:selectedClass" id="myForm:1" value="ConsumDaily">'
            f"{extras}"
        )

    def test_full(self):
        html = self._html(
            '<input name="myForm:selectedPlantID" value="PLANT42">'
            '<script id="scriptFrom">changeFromDate = function</script>'
            '<script id="scriptTo">assignToDate = function</script>'
        )
        state = LinzNetzFetcher._parse_initial_state(html)
        assert state.view_state == "vs1"
        assert state.granularity_field == "myForm:grid_eval:selectedClass"
        assert state.granularity_radio_indices == {
            "ConsumQuarter": "0",
            "ConsumDaily": "1",
        }
        assert state.plant_field == "myForm:selectedPlantID"
        assert state.plant_id == "PLANT42"
        assert state.from_date_source == "scriptFrom"
        assert state.to_date_source == "scriptTo"
        assert state.unit_field is None

    def test_no_plant_no_scripts(self):
        html = self._html()
        state = LinzNetzFetcher._parse_initial_state(html)
        assert state.plant_field is None
        assert state.plant_id is None
        assert state.from_date_source is None
        assert state.to_date_source is None

    def test_missing_granularity_radios(self):
        html = '<input name="jakarta.faces.ViewState" value="vs1">'
        with pytest.raises(FetchError, match="could not find granularity field"):
            LinzNetzFetcher._parse_initial_state(html)


class TestFindCsvButton:
    def test_found(self):
        xml = '<a id="myForm1:exportAreaID:0" href="#">CSV-Datei exportieren</a>'
        assert LinzNetzFetcher._find_csv_button(xml) == "myForm1:exportAreaID:0"

    def test_not_found(self):
        with pytest.raises(FetchError, match="CSV export button not found"):
            LinzNetzFetcher._find_csv_button("<div>nothing</div>")


class TestFetcherInit:
    def test_client_headers(self):
        f = LinzNetzFetcher("u", "p")
        assert f._client.headers["User-Agent"].startswith("Mozilla")
        assert f._client.headers["Accept-Language"] == "de-AT,de;q=0.9,en;q=0.8"


class TestFetcherContextManager:
    @pytest.mark.asyncio
    async def test_aclose_called(self):
        f = LinzNetzFetcher("u", "p")
        f._client.aclose = AsyncMock()
        async with f:
            pass
        f._client.aclose.assert_awaited_once()


class TestLoginAndGetConsumption:
    @pytest.mark.asyncio
    async def test_already_logged_in(self):
        f = LinzNetzFetcher("u", "p")
        f._client.get = AsyncMock(
            return_value=MagicMock(
                text='<form name="myForm1">consumption.jsf',
                url="https://services.linznetz.at/verbrauchsdateninformation/consumption.jsf",
            )
        )
        html, url = await f._login_and_get_consumption()
        assert "myForm1" in html
        assert "consumption.jsf" in url

    @pytest.mark.asyncio
    async def test_login_required(self):
        f = LinzNetzFetcher("u", "p")
        login_resp = MagicMock(
            text='<form name="myForm1">consumption.jsf',
            url="https://services.linznetz.at/verbrauchsdateninformation/consumption.jsf",
        )
        f._client.get = AsyncMock(
            return_value=MagicMock(
                text='<form action="https://idp/login-actions/authenticate">login</form>',
                url="https://idp/login-actions/authenticate",
            )
        )
        f._client.post = AsyncMock(return_value=login_resp)
        html, url = await f._login_and_get_consumption()
        assert "myForm1" in html
        f._client.post.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_wrong_landing_page(self):
        f = LinzNetzFetcher("u", "p")
        f._client.get = AsyncMock(
            return_value=MagicMock(text="ok", url="https://other")
        )
        with pytest.raises(FetchError, match="unexpected landing page"):
            await f._login_and_get_consumption()

    @pytest.mark.asyncio
    async def test_no_form(self):
        f = LinzNetzFetcher("u", "p")
        f._client.get = AsyncMock(
            return_value=MagicMock(
                text="consumption.jsf",
                url="https://services.linznetz.at/consumption.jsf",
            )
        )
        with pytest.raises(FetchError, match="consumption form not found"):
            await f._login_and_get_consumption()


class TestSelectGranularity:
    @pytest.mark.asyncio
    async def test_success(self):
        f = LinzNetzFetcher("u", "p")
        f._client.post = AsyncMock(
            return_value=MagicMock(
                text=(
                    '<update id="myForm1"><![CDATA[<input name="x:selectedClass" value="KWH">]]></update>'
                    '<update id="j_id1:javax.faces.ViewState:0"><![CDATA[vs2]]></update>'
                ),
                raise_for_status=lambda: None,
            )
        )
        state = FormState(
            view_state="vs1",
            granularity_field="gf",
            granularity_radio_indices={"ConsumQuarter": "0"},
            plant_field="pf",
            plant_id="pid",
            from_date_source="fs",
            to_date_source="ts",
            unit_field=None,
        )
        new_state = await f._select_granularity(
            state, "quarter", date(2024, 1, 1), date(2024, 1, 2)
        )
        assert new_state.view_state == "vs2"
        assert new_state.unit_field == "x:selectedClass"

    @pytest.mark.asyncio
    async def test_missing_radio_index(self):
        f = LinzNetzFetcher("u", "p")
        state = FormState(
            view_state="vs1",
            granularity_field="gf",
            granularity_radio_indices={},
            plant_field=None,
            plant_id=None,
            from_date_source=None,
            to_date_source=None,
            unit_field=None,
        )
        with pytest.raises(FetchError, match="granularity ConsumQuarter not in radios"):
            await f._select_granularity(
                state, "quarter", date(2024, 1, 1), date(2024, 1, 2)
            )

    @pytest.mark.asyncio
    async def test_no_myform1_update(self):
        f = LinzNetzFetcher("u", "p")
        f._client.post = AsyncMock(
            return_value=MagicMock(
                text='<update id="other">x</update>',
                raise_for_status=lambda: None,
            )
        )
        state = FormState(
            view_state="vs1",
            granularity_field="gf",
            granularity_radio_indices={"ConsumQuarter": "0"},
            plant_field=None,
            plant_id=None,
            from_date_source=None,
            to_date_source=None,
            unit_field=None,
        )
        with pytest.raises(FetchError, match="did not contain myForm1 update"):
            await f._select_granularity(
                state, "quarter", date(2024, 1, 1), date(2024, 1, 2)
            )


class TestSetCalendar:
    @pytest.mark.asyncio
    async def test_success(self):
        f = LinzNetzFetcher("u", "p")
        f._client.post = AsyncMock(
            return_value=MagicMock(
                text='<update id="j_id1:javax.faces.ViewState:0"><![CDATA[vs2]]></update>',
                raise_for_status=lambda: None,
            )
        )
        state = FormState(
            view_state="vs1",
            granularity_field="gf",
            granularity_radio_indices={},
            plant_field="pf",
            plant_id="pid",
            from_date_source="fs",
            to_date_source="ts",
            unit_field=None,
        )
        result = await f._set_calendar(state, "fs", "changeFromDate", "01.01.2024")
        assert result == "vs2"

    @pytest.mark.asyncio
    async def test_missing_plant(self):
        f = LinzNetzFetcher("u", "p")
        state = FormState(
            view_state="vs1",
            granularity_field="gf",
            granularity_radio_indices={},
            plant_field=None,
            plant_id=None,
            from_date_source=None,
            to_date_source=None,
            unit_field=None,
        )
        with pytest.raises(FetchError, match="plant field/id missing"):
            await f._set_calendar(state, "src", "param", "val")


class TestSetDates:
    @pytest.mark.asyncio
    async def test_success(self):
        f = LinzNetzFetcher("u", "p")
        f._client.post = AsyncMock(
            return_value=MagicMock(
                text='<update id="j_id1:javax.faces.ViewState:0"><![CDATA[vs_next]]></update>',
                raise_for_status=lambda: None,
            )
        )
        state = FormState(
            view_state="vs1",
            granularity_field="gf",
            granularity_radio_indices={},
            plant_field="pf",
            plant_id="pid",
            from_date_source="fs",
            to_date_source="ts",
            unit_field=None,
        )
        new_state = await f._set_dates(state, date(2024, 1, 1), date(2024, 1, 2))
        assert new_state.view_state == "vs_next"

    def test_missing_calendar_sources(self):
        f = LinzNetzFetcher("u", "p")
        state = FormState(
            view_state="vs1",
            granularity_field="gf",
            granularity_radio_indices={},
            plant_field="pf",
            plant_id="pid",
            from_date_source=None,
            to_date_source=None,
            unit_field=None,
        )
        with pytest.raises(FetchError, match="calendar widget script ids missing"):
            asyncio.run(f._set_dates(state, date(2024, 1, 1), date(2024, 1, 2)))


class TestClickDisplay:
    @pytest.mark.asyncio
    async def test_no_data(self):
        f = LinzNetzFetcher("u", "p")
        f._client.post = AsyncMock(
            return_value=MagicMock(
                text="<div>no export here</div>",
                raise_for_status=lambda: None,
            )
        )
        state = FormState(
            view_state="vs1",
            granularity_field="gf",
            granularity_radio_indices={"ConsumQuarter": "0"},
            plant_field=None,
            plant_id=None,
            from_date_source=None,
            to_date_source=None,
            unit_field=None,
        )
        with pytest.raises(NoDataError, match="no data available"):
            await f._click_display(
                state, date(2024, 1, 1), date(2024, 1, 2), "quarter", "KWH"
            )

    @pytest.mark.asyncio
    async def test_success(self):
        f = LinzNetzFetcher("u", "p")
        f._client.post = AsyncMock(
            return_value=MagicMock(
                text=(
                    '<div id="myForm1:list"><a id="myForm1:exportAreaID:0">CSV-Datei exportieren</a></div>'
                    '<update id="j_id1:javax.faces.ViewState:0"><![CDATA[vs2]]></update>'
                ),
                raise_for_status=lambda: None,
            )
        )
        state = FormState(
            view_state="vs1",
            granularity_field="gf",
            granularity_radio_indices={"ConsumQuarter": "0"},
            plant_field=None,
            plant_id=None,
            from_date_source=None,
            to_date_source=None,
            unit_field=None,
        )
        vs, btn = await f._click_display(
            state, date(2024, 1, 1), date(2024, 1, 2), "quarter", "KWH"
        )
        assert vs == "vs2"
        assert btn == "myForm1:exportAreaID:0"


class TestDownloadCsv:
    @pytest.mark.asyncio
    async def test_csv_response(self):
        f = LinzNetzFetcher("u", "p")
        f._client.post = AsyncMock(
            return_value=MagicMock(
                content=b"a,b",
                headers={"content-type": "text/csv"},
                raise_for_status=lambda: None,
            )
        )
        state = FormState(
            view_state="vs1",
            granularity_field="gf",
            granularity_radio_indices={},
            plant_field=None,
            plant_id=None,
            from_date_source=None,
            to_date_source=None,
            unit_field=None,
        )
        body, name = await f._download_csv(
            state, "vs2", "btn", date(2024, 1, 1), date(2024, 1, 2), "quarter", "KWH"
        )
        assert body == b"a,b"
        assert name == "consumption.csv"

    @pytest.mark.asyncio
    async def test_html_response(self):
        f = LinzNetzFetcher("u", "p")
        f._client.post = AsyncMock(
            return_value=MagicMock(
                content=b"<html>",
                headers={"content-type": "text/html"},
                raise_for_status=lambda: None,
            )
        )
        state = FormState(
            view_state="vs1",
            granularity_field="gf",
            granularity_radio_indices={},
            plant_field=None,
            plant_id=None,
            from_date_source=None,
            to_date_source=None,
            unit_field=None,
        )
        with pytest.raises(FetchError, match="expected CSV, got HTML"):
            await f._download_csv(
                state,
                "vs2",
                "btn",
                date(2024, 1, 1),
                date(2024, 1, 2),
                "quarter",
                "KWH",
            )

    @pytest.mark.asyncio
    async def test_filename_from_header(self):
        f = LinzNetzFetcher("u", "p")
        f._client.post = AsyncMock(
            return_value=MagicMock(
                content=b"x",
                headers={
                    "content-type": "text/csv",
                    "content-disposition": 'attachment; filename="my.csv"',
                },
                raise_for_status=lambda: None,
            )
        )
        state = FormState(
            view_state="vs1",
            granularity_field="gf",
            granularity_radio_indices={},
            plant_field=None,
            plant_id=None,
            from_date_source=None,
            to_date_source=None,
            unit_field=None,
        )
        _, name = await f._download_csv(
            state, "vs2", "btn", date(2024, 1, 1), date(2024, 1, 2), "quarter", "KWH"
        )
        assert name == "my.csv"


class TestFetch:
    @pytest.mark.asyncio
    async def test_invalid_date_range(self):
        f = LinzNetzFetcher("u", "p")
        with pytest.raises(ValueError, match="date_to must be >= date_from"):
            await f.fetch(date(2024, 1, 5), date(2024, 1, 1))


class TestParseDate:
    def test_valid(self):
        assert _parse_date("2024-01-15") == date(2024, 1, 15)

    def test_invalid(self):
        with pytest.raises(ValueError):
            _parse_date("not-a-date")


class TestAmain:
    @pytest.mark.asyncio
    async def test_missing_credentials(self, monkeypatch, capsys):
        monkeypatch.delenv("LINZNETZ_USERNAME", raising=False)
        monkeypatch.delenv("LINZNETZ_PASSWORD", raising=False)
        args = argparse.Namespace(
            username=None,
            password=None,
            date_from=None,
            date_to=None,
            granularity="quarter",
            output=None,
            verbose=False,
        )
        assert await _amain(args) == 2
        captured = capsys.readouterr()
        assert "credentials missing" in captured.err

    @pytest.mark.asyncio
    async def test_success(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LINZNETZ_USERNAME", "u")
        monkeypatch.setenv("LINZNETZ_PASSWORD", "p")
        out = tmp_path / "out.csv"
        args = argparse.Namespace(
            username=None,
            password=None,
            date_from="2024-01-01",
            date_to="2024-01-02",
            granularity="quarter",
            output=str(out),
            verbose=False,
        )
        with patch.object(
            LinzNetzFetcher, "fetch", return_value=(b"csv,data", "server.csv")
        ) as mock_fetch:
            assert await _amain(args) == 0
            mock_fetch.assert_awaited_once()
        assert out.read_bytes() == b"csv,data"

    @pytest.mark.asyncio
    async def test_no_data(self, monkeypatch, capsys):
        monkeypatch.setenv("LINZNETZ_USERNAME", "u")
        monkeypatch.setenv("LINZNETZ_PASSWORD", "p")
        args = argparse.Namespace(
            username=None,
            password=None,
            date_from=None,
            date_to=None,
            granularity="quarter",
            output=None,
            verbose=False,
        )
        with patch.object(LinzNetzFetcher, "fetch", side_effect=NoDataError("no data")):
            assert await _amain(args) == 3
        assert "no data" in capsys.readouterr().err

    @pytest.mark.asyncio
    async def test_fetch_error(self, monkeypatch, capsys):
        monkeypatch.setenv("LINZNETZ_USERNAME", "u")
        monkeypatch.setenv("LINZNETZ_PASSWORD", "p")
        args = argparse.Namespace(
            username=None,
            password=None,
            date_from=None,
            date_to=None,
            granularity="quarter",
            output=None,
            verbose=False,
        )
        with patch.object(LinzNetzFetcher, "fetch", side_effect=FetchError("boom")):
            assert await _amain(args) == 1
        assert "boom" in capsys.readouterr().err


class TestMain:
    def test_help(self, capsys):
        with pytest.raises(SystemExit) as exc_info:
            with patch.object(sys, "argv", ["linznetz_fetcher", "--help"]):
                main()
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "--username" in captured.out
