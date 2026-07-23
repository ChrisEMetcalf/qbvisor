import asyncio
import inspect
import warnings
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from qbvisor import QueryHelper, QuickBaseClient

EXPECTED_SIGNATURES = {
    "download_attachments_async": (
        "(self, app_name: str, table_name: str, file_field_label: str, target_dir: str, "
        "where: str | None = None, max_concurrency: int = 4, page_size: int = 1000) "
        "-> list[dict[str, Any]]"
    ),
    "download_table_attachments_async": (
        "(self, app_name: str, table_name: str, target_dir: str, where: str | None = None, "
        "max_concurrency: int = 4, page_size: int = 1000) -> list[dict[str, Any]]"
    ),
    "get_field_id": "(self, app_id: str, table_id: str, field_label: str) -> int",
    "get_table_id": "(self, app_id: str, table_id: str) -> str",
    "get_field": "(self, app_id, table_id, field_id)",
    "summarize_config": "(self, show_fields: bool = False)",
    "dump_full_config": "(self)",
    "download_records_to_csv": (
        "(self, app_name: str, table_name: str, output_dir: str, "
        "where: str = \"{3.GT.'0'}\", chunk_size: int = 1000, "
        "record_limit: int | None = None, max_concurrency: int = 4) -> str"
    ),
}

LEDGER_ENTRIES = {
    "download_attachments_async": "Compatibility-retained synchronous helper",
    "download_table_attachments_async": "Compatibility-retained synchronous helper",
    "get_field_id": "Compatibility-retained metadata helper",
    "get_table_id": "Compatibility-retained metadata helper",
    "get_field": "Compatibility-retained metadata helper",
    "summarize_config": "Compatibility-retained diagnostic helper",
    "dump_full_config": "Compatibility-retained diagnostic helper",
    "download_records_to_csv(max_concurrency=...)": "compatibility-only parameter",
}


def _normalized_signature(method_name: str) -> str:
    signature = str(inspect.signature(getattr(QuickBaseClient, method_name)))
    for module_prefix in ("typing.", "collections.abc.", "qbvisor.models."):
        signature = signature.replace(module_prefix, "")
    return signature


def _ledger_section(document: str, entry: str) -> str:
    heading = f"### `{entry}`"
    start = document.index(heading)
    next_heading = document.find("\n### `", start + len(heading))
    return document[start:] if next_heading == -1 else document[start:next_heading]


def test_scoped_compatibility_entry_points_preserve_signature_snapshots():
    actual = {name: _normalized_signature(name) for name in EXPECTED_SIGNATURES}

    assert actual == EXPECTED_SIGNATURES


def test_ledger_classifies_every_scoped_entry_and_records_contract_fields():
    ledger = (Path(__file__).resolve().parents[1] / "docs" / "compatibility-helpers.md").read_text(
        encoding="utf-8"
    )

    for entry, classification in LEDGER_ENTRIES.items():
        section = _ledger_section(ledger, entry)
        assert "**Classification:**" in section
        assert classification in section
        assert "**Signature:**" in section
        assert "**Behavior:**" in section
        assert "**Side effects:**" in section
        assert "**Preferred alternative:**" in section


@pytest.mark.parametrize(
    ("method_name", "arguments"),
    [
        (
            "download_attachments_async",
            ("Billing", "Invoices", "Source PDF"),
        ),
        (
            "download_table_attachments_async",
            ("Billing", "Invoices"),
        ),
    ],
)
def test_async_named_sync_helpers_reject_active_event_loop_before_side_effects(
    tmp_path, method_name, arguments
):
    client = QuickBaseClient.__new__(QuickBaseClient)
    client._ids = Mock()
    target_dir = tmp_path / method_name

    async def invoke() -> None:
        with pytest.raises(
            RuntimeError,
            match=rf"{method_name}\(\) is a compatibility-retained synchronous method",
        ):
            getattr(client, method_name)(*arguments, str(target_dir))

    asyncio.run(invoke())

    client._ids.assert_not_called()
    assert not target_dir.exists()


def test_explicit_nondefault_csv_concurrency_warns_before_export_side_effects(tmp_path):
    client = QuickBaseClient.__new__(QuickBaseClient)
    client._ids = Mock(side_effect=RuntimeError("stop after compatibility warning"))

    with pytest.warns(UserWarning, match="compatibility-only parameter and is ignored"):
        with pytest.raises(RuntimeError, match="stop after compatibility warning"):
            client.download_records_to_csv(
                "Billing",
                "Invoices",
                str(tmp_path),
                max_concurrency=8,
            )

    client._ids.assert_called_once_with("Billing", "Invoices")
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("passing_style", ["keyword", "positional"])
def test_explicit_default_csv_concurrency_warns(passing_style, tmp_path):
    client = QuickBaseClient.__new__(QuickBaseClient)
    client._ids = Mock(side_effect=RuntimeError("stop after compatibility warning"))

    with pytest.warns(UserWarning, match="compatibility-only parameter and is ignored"):
        with pytest.raises(RuntimeError, match="stop after compatibility warning"):
            if passing_style == "keyword":
                client.download_records_to_csv(
                    "Billing",
                    "Invoices",
                    str(tmp_path),
                    max_concurrency=4,
                )
            else:
                client.download_records_to_csv(
                    "Billing",
                    "Invoices",
                    str(tmp_path),
                    "{3.GT.'0'}",
                    1000,
                    None,
                    4,
                )

    client._ids.assert_called_once_with("Billing", "Invoices")


def test_default_csv_concurrency_is_silent_when_the_parameter_is_omitted(tmp_path):
    client = QuickBaseClient.__new__(QuickBaseClient)
    client._ids = Mock(side_effect=RuntimeError("stop after compatibility check"))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(RuntimeError, match="stop after compatibility check"):
            client.download_records_to_csv("Billing", "Invoices", str(tmp_path))

    assert not caught


def test_preferred_beginner_path_builds_query_from_label_and_uses_facade():
    client = QuickBaseClient.__new__(QuickBaseClient)
    client.meta = Mock()
    client.meta.get_field_map.return_value = {
        "Invoice Number": {"id": 6, "type": "text"},
        "Status": {"id": 7, "type": "text"},
    }
    client.query_dataframe = Mock(return_value="active invoices")

    query = QueryHelper(client, "Billing", "Invoices")
    active_filter = query.eq("Status", "Active")
    result = client.query_dataframe(
        "Billing",
        "Invoices",
        ["Invoice Number", "Status"],
        where=active_filter,
    )

    assert active_filter == "{7.EX.'Active'}"
    assert result == "active invoices"
    client.meta.get_field_map.assert_called_once_with("Billing", "Invoices")
    client.query_dataframe.assert_called_once_with(
        "Billing",
        "Invoices",
        ["Invoice Number", "Status"],
        where="{7.EX.'Active'}",
    )


def test_metadata_helpers_return_values_and_propagate_errors():
    client = QuickBaseClient.__new__(QuickBaseClient)
    client._fields = Mock()
    client._tables = Mock()
    client._fields.get_id.return_value = 7
    client._tables.get_id.return_value = "tbl_invoices"
    client._fields.get.return_value = {"id": 7, "label": "Status"}

    assert client.get_field_id("app_billing", "tbl_invoices", "Status") == 7
    assert client.get_table_id("app_billing", "Invoices") == "tbl_invoices"
    assert client.get_field("app_billing", "Invoices", 7) == {"id": 7, "label": "Status"}

    failure = RuntimeError("metadata unavailable")
    client._fields.get.side_effect = failure
    with pytest.raises(RuntimeError, match="metadata unavailable"):
        client.get_field("app_billing", "Invoices", 7)


def test_config_diagnostics_report_cached_success_and_serialization_error():
    client = QuickBaseClient.__new__(QuickBaseClient)
    client.logger = Mock()
    client.meta = SimpleNamespace(
        cache={
            "Billing": {
                "tables": {
                    "Invoices": {
                        "id": "tbl_invoices",
                        "size": 2,
                        "fields": {"Status": {"id": 7, "type": "text"}},
                    }
                }
            }
        }
    )

    client.summarize_config(show_fields=True)
    summary = "\n".join(call.args[0] for call in client.logger.info.call_args_list)
    assert "QuickBase Config Overview:" in summary
    assert "Invoices" in summary
    assert "Status" in summary

    client.logger.reset_mock()
    client.dump_full_config()
    assert '"tbl_invoices"' in client.logger.info.call_args.args[0]

    client.logger.reset_mock()
    client.meta.cache = {"not-json": object()}
    client.dump_full_config()
    client.logger.error.assert_called_once()
    assert "Failed to serialize config" in client.logger.error.call_args.args[0]
