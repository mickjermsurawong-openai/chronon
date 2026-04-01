from ai.chronon.staging_query import EngineType, StagingQuery, TableDependency


def get_select_star_export(table: str, partition_column: str = "ds"):
    snowflake_export_sql = f"""
    SELECT
      * EXCLUDE ({partition_column}),
       DATE_TRUNC('DAY', {partition_column})::DATE as ds
    FROM {table}
    WHERE
    DATE_TRUNC('DAY', {partition_column}) BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """

    return StagingQuery(
        query=snowflake_export_sql,
        output_namespace="data",
        engine_type=EngineType.SNOWFLAKE,
        dependencies=[
            TableDependency(table=f"{table}", partition_column=partition_column, offset=0)
        ],
        version=0,
        step_days=30,
    )


def get_native_partition_export(table: str, partition_column: str, time_partitioned: bool = None):
    snowflake_partition_sql = f"""
    SELECT
        *,
        DATE_TRUNC('DAY', {partition_column})::DATE as ds
    FROM {table}
    WHERE
    {partition_column} BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """
    return StagingQuery(
        query=snowflake_partition_sql,
        output_namespace="data",
        engine_type=EngineType.SNOWFLAKE,
        dependencies=[
            TableDependency(table=f"{table}", partition_column=partition_column, offset=0, time_partitioned=time_partitioned)
        ],
        version=0,
        step_days=30,
    )



user_activities = get_native_partition_export("user_activities", "ds")
checkouts = get_native_partition_export("checkouts", "ds")
dim_listings_pc = get_native_partition_export("dim_listings_custom_part", "datest", time_partitioned=True)
dim_listings = get_select_star_export("dim_listings", "ds")
dim_merchants = get_select_star_export("dim_merchants", "ds")
dim_users = get_select_star_export("dim_users", "ds")
