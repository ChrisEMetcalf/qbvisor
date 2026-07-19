# Client

`QuickBaseClient` is the supported high-level facade. Application, table, and field parameters
accept configured labels where the method documentation uses `app_name`, `table_name`, or a field
label. Direct Quickbase IDs are also accepted where described in the configuration guide.

::: qbvisor.client.QuickBaseClient
    options:
      members:
        - close
        - backup_app
        - plan_app
        - apply_app
        - create_app
        - get_app
        - get_app_events
        - get_app_roles
        - update_app
        - delete_app
        - copy_app
        - create_table
        - get_tables_for_app
        - get_table
        - update_table
        - delete_table
        - get_all_relationships
        - create_relationship
        - update_relationship
        - delete_relationship
        - get_reports_for_table
        - get_report
        - run_report
        - get_fields_for_table
        - create_field
        - delete_fields
        - get_fields_usage
        - get_field_usage
        - run_formula
        - upsert_records
        - delete_records
        - query_records
        - records_modified_since
        - query_dataframe
        - download_records_to_csv
        - get_file_attachment_fields
        - delete_file
        - download_attachments_async
        - download_attachment_base64
        - download_table_attachments_async
        - get_field_id
        - get_table_id
        - get_field
        - summarize_config
        - dump_full_config
