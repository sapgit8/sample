import argparse
import shlex
import logging
import sys
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.dbutils import DBUtils
import pyspark.sql.functions as F
from oacis_spark.utilities.utils import (
    check_dbfs_path,
    configure_storage,
    get_cols_to_front,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()],
)


def transform_raw(spark_dataframe: DataFrame) -> DataFrame:
    """transform raw unaltered file by adding metadata

    Args:
        spark_dataframe (DataFrame): spark dataframe to add metadata to

    Returns:
        DataFrame: a spark dataframe
    """
    raw_transformed = spark_dataframe.withColumn(
        "SOURCE_FILE_NAME", F.input_file_name()
    )

    return raw_transformed


def process_landing_to_raw(
    spark: SparkSession, source_path: str, raw_table: str, write_table: bool = False
) -> DataFrame:
    """Takes the most recent extract from source_path and read it as parquet.
    Parse the data and apply raw transformations and write as raw_table.

    Args:
        spark (SparkSession): spark session
        source_path (str): raw landing dir for data
        raw_table (str): table name to write/overwrite
        write_table (bool, optional): _description_. Defaults to False.

    Returns:
        DataFrame: _description_
    """
    # get dbutils
    dbutils = DBUtils(spark)

    # get most recent table extract
    daily_table_files = [x[0] for x in dbutils.fs.ls(source_path)]
    most_recent_table = max(daily_table_files)

    # read the most recent table extract
    logging.info(f"Reading source parquet {most_recent_table}")
    raw_df = spark.read.option("header", True).parquet(most_recent_table)

    # transform the data
    raw_df = transform_raw(spark_dataframe=raw_df)

    if write_table:
        logging.info(f"Writing raw table")
        raw_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            raw_table
        )

    return raw_df


def parse_raw_sps(
    spark: SparkSession,
    spark_dataframe: DataFrame,
    bronze_table_name: str,
    write_table: bool = False,
) -> DataFrame:
    """Parse the raw table to the bronze table

    Args:
        spark (SparkSession): spark session
        spark_dataframe (DataFrame): raw spark df of sps
        bronze_table_name (str): tbale name to write out
        write_table (bool, optional): write table (True) or just return spark df (False). Defaults to False.

    Returns:
        DataFrame: _description_
    """
    # get the level, node, and time columns
    node_columns = [
        x
        for x in spark_dataframe.columns
        if x.startswith("Level_") & x.endswith("Name")
    ]
    other_columns = [
        "LEVEL_00_NODEID",
        "WORK_STATION_ATTR_VAL",
        "WORK_STATION_ATTR_VAL_INFO",
        "WORK_CENTER_ATTR_VAL",
        "WORK_CENTER_ATTR_VAL_INFO",
        "IS_CONSTRAINED",
    ]
    all_columns = node_columns + other_columns

    # check that the level_00 column is there as it represents work stations
    assert (
        "Level_00_LevelName" in node_columns
    ), "missing Level_00_LevelName column which is required for parsing"

    # only get rows that are rooted form the work station
    staging_df = spark_dataframe.filter(
        F.col("Level_00_LevelName") == "Work Station"
    ).select(all_columns)

    # get new columns names the from the LevelNameXX fields
    column_names = [x for x in node_columns if x.endswith("LevelName")]
    level_columns = staging_df.select(column_names).limit(1).collect()
    new_column_names = [
        x.upper().replace(" ", "_") for x in level_columns[0] if x != None
    ]

    # get columns with the actual values in them
    value_names = [x for x in node_columns if x.endswith("NodeName")]

    # rename all the columns to the new column names
    for x in list(zip(value_names, new_column_names)):
        staging_df = staging_df.withColumnRenamed(x[0], x[1])

    # select only the stuff we care about
    staging_df = (
        staging_df.select(new_column_names + other_columns)
        .withColumnRenamed("SUB-DOMAIN", "SUB_DOMAIN")
        .withColumnRenamed("SUB-DIVISION", "SUB_DIVISION")
        .withColumnRenamed("LEVEL_00_NODEID", "WORK_STATION_NODE_ID")
        .fillna({"IS_CONSTRAINED": "No"})
        .fillna("")
    )

    key_cols = ["WORK_STATION_ATTR_VAL"]
    # failure test cols
    # key_cols = ['SUB-DOMAIN', 'DOMAIN']

    try:
        results = staging_df.groupBy(key_cols).count().filter("count > 1").collect()
        assert len(results) == 0
    except:
        logging.error(f"non unique rows found given key cols {key_cols}")
        print(results)
        raise

    bronze_sdf = get_cols_to_front(
        staging_df, ["WORK_STATION_ATTR_VAL", "ATTRIBUTE_VALUE_INFO"]
    )

    if write_table:
        logging.info(f"Writing raw table")
        bronze_sdf.write.mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(bronze_table_name)

        bronze_sdf = spark.read.table(bronze_table_name)

    return bronze_sdf


class SPSHierarchy:
    def __init__(self, env: dict, incremental: bool) -> None:
        """Class for processing weight gain data into the warehouse

        1. Kill and fill raw
        2. merger raw into bronze and clean/standardize
        3. merge bronze into warehouse dimensions
        4. merge bronze into fact table

        Args:
            env (dict): dictionary containing 'schema' item
            incremental (bool): True = build incrementally
        """
        logging.info("Getting Spark Session")
        self.spark = SparkSession.builder.getOrCreate()
        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        self.schema = env["schema"]
        self.raw_schema = "raw_" + env["schema"]
        self.bronze_schema = "bronze_" + env["schema"]
        self.silver_schema = "silver_" + env["schema"]
        self.incremental = incremental

    def _sps_build(self):
        """Build the sps hierearchy pipeline"""
        # make ADLS accessible
        configure_storage(
            spark=self.spark,
            scope_name="KVP_DBX",
            storage_account_secret_name="storage-bronze-name",
            storage_key_secret_name="storage-bronze",
        )

        # get dbutils
        dbutils = DBUtils(self.spark)

        # check data source path is accessible and exists
        storage_account = dbutils.secrets.get("KVP_DBX", "storage-bronze-name")
        data_source_path = f"wasbs://raw@{storage_account}.blob.core.usgovcloudapi.net/landing/sps_hierarchy/"
        check_dbfs_path(self.spark, data_source_path)

        # create table name for each schema
        table_name = "sps_hierarchy"
        raw_table = f"raw_oacis.{table_name}"
        bronze_table = f"bronze_oacis.{table_name}"
        silver_table = f"silver_oacis.{table_name}"

        logging.info("Processing Landing to Raw")
        raw_df = process_landing_to_raw(
            spark=self.spark,
            source_path=data_source_path,
            raw_table=raw_table,
            write_table=True,
        )

        logging.info("Processing Raw to Bronze")
        bronze_df = parse_raw_sps(
            spark=self.spark,
            spark_dataframe=raw_df,
            bronze_table_name=bronze_table,
            write_table=True,
        )

        source_df = (
            bronze_df
            .withColumn("EFFECTIVE_FROM", F.current_date())
            .withColumn("EFFECTIVE_TO", F.lit("9999-12-31").cast("date"))
            .withColumn("CURR_REC_IND", F.lit("Y"))
            .withColumn("DWH_LOAD_TS", F.current_timestamp())
        )

        # write out table if not exists or for a full refresh
        if not self.spark.catalog.tableExists(silver_table) or not self.incremental:
            # if writing out for the first time then change the effective date to the beginning of time
            logging.info(f"Overwriting bronze table: {silver_table}")
            (
                source_df
                .write.mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(silver_table)
            )

        # set reference to silver talbe to merge into
        target_df = self.spark.read.table(silver_table)

        id_cols = ['WORK_STATION_ATTR_VAL']

        meta_cols = ['EFFECTIVE_FROM', 'EFFECTIVE_TO', 'DWH_LOAD_TS', 'CURR_REC_IND']

        attribute_cols = [x for x in source_df.columns if x not in id_cols + meta_cols]

        # update curr_rec_ind clause
        rec_ind_update_clause = " or ".join(
            [f"source.{x} != target.{x}" for x in attribute_cols]
        )

        # Step 1: Updated Target Records that have new source values
        updated_target_records = (
            target_df.alias("target")
            .join(source_df.alias("source"), id_cols)
            .filter(f"target.CURR_REC_IND = 'Y' AND ({rec_ind_update_clause})")
            .select('target.*')
            .selectExpr(
            *id_cols,
            *attribute_cols,
            "target.EFFECTIVE_FROM",
            "current_date() as EFFECTIVE_TO",
            "'N' as CURR_REC_IND",
            "target.DWH_LOAD_TS"
            )
        )

        # step 2 - get new versions of target records
        new_changed_source_records = source_df.alias('source').join(updated_target_records.alias('target'), id_cols, 'inner').select('source.*')

        # step 3 - get other versions of updated records
        old_records_of_changed_records = target_df.where('CURR_REC_IND = "N"').alias('target').join(new_changed_source_records.alias('new_source'), id_cols, 'inner').select('target.*')

        # step 4 - get new records that dont exist in target
        new_records = source_df.alias('source').join(target_df.alias('target'), id_cols, 'leftanti').select('source.*')

        # Step 5: all other records not being updated

        all_other_records = target_df.alias('target').join(updated_target_records.alias('updated_target'), id_cols, 'leftanti').select('target.*')

        # step 6: combine

        final_df = (
            updated_target_records
            .unionByName(new_changed_source_records)
            .unionByName(old_records_of_changed_records)
            .unionByName(new_records)
            .unionByName(all_other_records)
        )

        # Step 4: Write the final DataFrame back to the Delta table
        final_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)

        source_bt_list = (
            bronze_df
            .select('work_station_attr_val')
            .distinct()
            .rdd.map(lambda x: x[0])
            .collect()
        )

        delta_table = DeltaTable.forName(sparkSession=self.spark, tableOrViewName=silver_table)

        delta_table.update(
            condition = "work_station_attr_val NOT IN ('" + "','".join(source_bt_list) + "') and CURR_REC_IND = 'Y'",
            set={"CURR_REC_IND": F.lit("N"), "EFFECTIVE_TO": F.current_date()}
        )

    def _target_utilization_build(self):
        """Build the target utlization pipeline"""
        # make ADLS accessible
        configure_storage(
            spark=self.spark,
            scope_name="KVP_DBX",
            storage_account_secret_name="storage-bronze-name",
            storage_key_secret_name="storage-bronze",
        )

        # get dbutils
        dbutils = DBUtils(self.spark)

        # check data source path is accessible and exists
        storage_account = dbutils.secrets.get("KVP_DBX", "storage-bronze-name")
        data_source_path = f"wasbs://raw@{storage_account}.blob.core.usgovcloudapi.net/landing/target_utilization/"
        check_dbfs_path(self.spark, data_source_path)

        table_name = "target_utilization"
        raw_table = f"raw_oacis.{table_name}"
        bronze_table = f"bronze_oacis.{table_name}"
        silver_table = f"silver_oacis.{table_name}"

        logging.info("Processing Landing to Raw")
        raw_df = process_landing_to_raw(
            spark=self.spark,
            source_path=data_source_path,
            raw_table=raw_table,
            write_table=True,
        )

        # write bronze table
        (
            raw_df.dropna()
            .drop("SOURCE_FILE_NAME")
            .write.mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(bronze_table)
        )

        # CURRENTLY NO TRANSOFMATIONS FROM BRONZE -> SILVER

        # write silver table
        (
            self.spark.read.table(bronze_table)
            .write.mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(silver_table)
        )


def main():
    parser = argparse.ArgumentParser(description="Parse bool")
    parser.add_argument(
        "--full-refresh",
        default=True,
        action="store_false",
        help="Build from scratch?",
    )
    parser.add_argument("--schema", type=str, required=True)
    args = parser.parse_args(shlex.split(" ".join(sys.argv[1:])))

    logging.info("Starting SPS Build")
    logging.info(f"Building Warehouse Incrementally? : {args.full_refresh}")

    sps_hierarchy = SPSHierarchy(
        env={"schema": args.schema}, incremental=args.full_refresh
    )

    logging.info("Loading SPS Hierarchy")
    sps_hierarchy._sps_build()
    logging.info("Finished building SPS Hierarchy")

    logging.info("Loading SPS Target Utilization")
    sps_hierarchy._target_utilization_build()
    logging.info("Finished building SPS Target Utilization")

    logging.info("Finished building SPS")


if __name__ == "__main__":
    main()
