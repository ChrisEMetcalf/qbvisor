from src import QuickBaseClient, QueryHelper


def main():
    qb = QuickBaseClient()

    app_name = "Atmos"
    table_name = "Daily Tracking 2.0"

    # ---------------------------
    # Basic Query
    # ---------------------------
    df = qb.query_dataframe(
        app_name=app_name,
        table_name=table_name,
        select_fields=['Date', 'Roster - Full Name', 'Job Name'],
        where="{6.EX.'2025-05-13'}"
    )
    print("Raw DataFrame:")
    print(df.head())

    # ---------------------------
    # Using QueryHelper
    # ---------------------------
    q = QueryHelper(qb, app_name, table_name)
    where_clause = q.eq('Roster - Full Name', 'Emmanuel Barrios')

    df_filtered = qb.query_dataframe(
        app_name=app_name,
        table_name=table_name,
        select_fields=['Date', 'Roster - Full Name', 'Job Name'],
        where=where_clause
    )
    print("Filtered DataFrame:")
    print(df_filtered.head())

    # ---------------------------
    # Exporting to CSV
    # ---------------------------
    out_path = qb.download_records_to_csv(
        app_name=app_name,
        table_name=table_name,
        where=where_clause,
        output_dir='.',
        max_concurrency=2
    )
    print(f"CSV file saved to: {out_path}")

if __name__ == "__main__":
    main()