import qbvisor


def test_existing_public_exports_remain_available():
    expected_exports = {
        "QuickBaseClient",
        "QuickBaseTransport",
        "RetryPolicy",
        "QueryHelper",
        "sanitize_filenames",
        "ensure_temp_dir",
        "generate_timestamped_folder",
        "summarize_file_sizes",
        "LoggingConfigurator",
        "get_logger",
    }

    assert expected_exports <= set(qbvisor.__all__)
    for name in expected_exports:
        assert hasattr(qbvisor, name)
