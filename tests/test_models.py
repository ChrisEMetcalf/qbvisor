import pytest

from qbvisor.models import RelationshipSummary


def test_relationship_summary_requires_a_field_except_for_count():
    with pytest.raises(ValueError, match="SUM summaries require a field"):
        RelationshipSummary("SUM")

    assert RelationshipSummary("COUNT").field is None
    assert RelationshipSummary("COUNT", 0).field == 0


def test_count_summary_rejects_a_nonzero_field():
    with pytest.raises(ValueError, match="COUNT summaries must omit field"):
        RelationshipSummary("COUNT", "Hours")


def test_relationship_summary_rejects_an_unknown_accumulation_type():
    with pytest.raises(ValueError, match="Unsupported accumulation type"):
        RelationshipSummary("MEDIAN", "Hours")  # type: ignore[arg-type]
