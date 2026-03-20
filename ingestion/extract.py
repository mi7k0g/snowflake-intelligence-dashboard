# For this project, QUERY_HISTORY and WAREHOUSE_METERING_HISTORY are available
# directly in Snowflake via SNOWFLAKE.ACCOUNT_USAGE and can be read natively in dbt.
# This script simulates a real-world ingestion scenario where data comes from an
# external system (e.g. a third-party monitoring tool, an API, or an on-prem database)
# and needs to be loaded into Snowflake before dbt can transform it.


import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """Create and return a Snowflake connection."""
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def extract_query_history(conn, days: int = 30) -> pd.DataFrame:
    """Extract query history from Snowflake ACCOUNT_USAGE."""
    query = f"""
        SELECT
            QUERY_ID,
            QUERY_TEXT,
            USER_NAME,
            WAREHOUSE_NAME,
            EXECUTION_STATUS,
            TOTAL_ELAPSED_TIME,
            CREDITS_USED_CLOUD_SERVICES,
            START_TIME,
            END_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    """
    cursor = conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    cursor.close()
    return df


def extract_warehouse_metering(conn, days: int = 30) -> pd.DataFrame:
    """Extract warehouse metering history from Snowflake ACCOUNT_USAGE."""
    query = f"""
        SELECT
            WAREHOUSE_NAME,
            CREDITS_USED,
            CREDITS_USED_COMPUTE,
            CREDITS_USED_CLOUD_SERVICES,
            START_TIME,
            END_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    """
    cursor = conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    cursor.close()
    return df


def load_to_snowflake(conn, df: pd.DataFrame, target_table: str) -> None:
    """Load a DataFrame into a Snowflake raw table."""
    from snowflake.connector.pandas_tools import write_pandas

    success, _, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=target_table,
        auto_create_table=True,
        overwrite=True,
    )
    if success:
        print(f"Loaded {nrows} rows into {target_table}")
    else:
        raise Exception(f"Failed to load data into {target_table}")


def main():
    conn = get_connection()
    try:
        print("Extracting query history...")
        query_history = extract_query_history(conn)
        load_to_snowflake(conn, query_history, "RAW_QUERY_HISTORY")

        print("Extracting warehouse metering...")
        warehouse_metering = extract_warehouse_metering(conn)
        load_to_snowflake(conn, warehouse_metering, "RAW_WAREHOUSE_METERING")
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        conn.close()
        print("Done.")


if __name__ == "__main__":
    main()
