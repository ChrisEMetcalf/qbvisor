import qbvisor


def test_existing_public_exports_remain_available():
    expected_exports = {
        "QuickBaseClient",
        "QueryHelper",
        "sanitize_filenames",
        "ensure_temp_dir",
        "generate_timestamped_folder",
        "summarize_file_sizes",
        "LoggingConfigurator",
        "get_logger",
    }

    assert set(qbvisor.__all__) == expected_exports
    for name in expected_exports:
        assert hasattr(qbvisor, name)
