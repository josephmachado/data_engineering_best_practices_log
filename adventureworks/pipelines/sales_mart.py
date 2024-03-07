import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import great_expectations as gx
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit

try:
    from adventureworks.pipelines.utils.create_fake_data import generate_bronze_data
except ModuleNotFoundError:
    from utils.create_fake_data import generate_bronze_data  # type: ignore

import inspect
import psycopg2
from logging import Logger
from typing import Dict, Any, Optional


class LoggerProvider:
    def get_logger(self, spark: SparkSession, custom_prefix: Optional[str] = ""):
        log4j_logger = spark._jvm.org.apache.log4j  # noqa
        return log4j_logger.LogManager.getLogger(custom_prefix + self.__full_name__())

    def __full_name__(self):
        klass = self.__class__
        module = klass.__module__
        if module == "__builtin__":
            return klass.__name__  # avoid outputs like '__builtin__.str'
        return module + "." + klass.__name__


def log_metadata(func):
    def log_wrapper(*args, **kwargs):
        input_params = dict(
            zip(list(locals().keys())[:-1], list(locals().values())[:-1])
        )
        param_names = list(
            inspect.signature(func).parameters.keys()
        )  # order is preserved
        # match with input_params.get('args') and
        # then input_params.get('kwargs')
        input_dict = {}
        for v in input_params.get("args"):
            input_dict[param_names.pop(0)] = v

        # json_data = json.dumps(input_params.get("kwargs"))
        # log timestamp, function name, run id,
        conn = psycopg2.connect(
            host="metadata",
            database="metadatadb",
            user="sdeuser",
            password="sdepassword",
            port="5432",
        )
        # Create a cursor object
        cur = conn.cursor()

        # Execute the CREATE TABLE IF NOT EXISTS statement
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS run_metadata (
            run_id VARCHAR,
            pipeline_id VARCHAR,
            run_params VARCHAR,
            ts_inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        )

        # Commit the transaction
        conn.commit()
        # Execute a simple SQL query
        # Define the INSERT query
        insert_query = """
        INSERT INTO run_metadata (run_id, pipeline_id, run_params)
        VALUES (%s, %s, %s);
        """
        # Define the data to be inserted
        run_id = input_params.get("kwargs").get("run_id")
        pipeline_id = input_params.get("kwargs").get("pipeline_id")
        run_params = str(
            input_dict | input_params.get("kwargs") | {"function": func.__name__}
        )

        # Execute the INSERT query
        cur.execute(insert_query, (run_id, pipeline_id, run_params))

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        cur.close()
        conn.close()

        return func(*args, **kwargs)

    return log_wrapper


@dataclass
class DeltaDataSet:
    name: str
    curr_data: DataFrame
    primary_keys: List[str]
    storage_path: str
    table_name: str
    data_type: str
    database: str
    partition: str
    skip_publish: bool = False
    replace_partition: bool = False


class InValidDataException(Exception):
    pass


class StandardETL(ABC):
    def __init__(
        self,
        storage_path: Optional[str] = None,
        database: Optional[str] = None,
        partition: Optional[str] = None,
    ):
        self.STORAGE_PATH = storage_path or "s3a://adventureworks/delta"
        self.DATABASE = database or "adventureworks"
        self.DEFAULT_PARTITION = partition or datetime.now().strftime(
            "%Y-%m-%d-%H-%M-%S"
        )

    def run_data_validations(self, input_datasets: Dict[str, DeltaDataSet]):
        context = gx.get_context(
            context_root_dir=os.path.join(
                os.getcwd(),
                "adventureworks",
                "great_expectations",
            )
        )

        validations = []
        for input_dataset in input_datasets.values():
            validations.append(
                {
                    "batch_request": context.get_datasource("spark_datasource")
                    .get_asset(input_dataset.name)
                    .build_batch_request(dataframe=input_dataset.curr_data),
                    "expectation_suite_name": input_dataset.name,
                }
            )
        return context.run_checkpoint(
            checkpoint_name="dq_checkpoint", validations=validations
        ).list_validation_results()

    @log_metadata
    def validate_data(self, input_datasets: Dict[str, DeltaDataSet], **kwargs) -> bool:
        results = {}
        for validation in self.run_data_validations(input_datasets):
            results[validation.get("meta").get("expectation_suite_name")] = (
                validation.get("success")
            )
        for k, v in results.items():
            if not v:
                raise InValidDataException(
                    f"The {k} dataset did not pass validation, please check"
                    " the metadata db for more information"
                )

        return True

    def check_required_inputs(
        self, input_datasets: Dict[str, DeltaDataSet], required_ds: List[str]
    ) -> None:
        if not all([ds in input_datasets for ds in required_ds]):
            raise ValueError(
                f"The input_datasets {input_datasets.keys()} does not contain"
                f" {required_ds}"
            )

    def construct_join_string(self, keys: List[str]) -> str:
        return " AND ".join([f"target.{key} = source.{key}" for key in keys])

    def publish_data(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        spark: SparkSession,
        **kwargs,
    ) -> None:
        for input_dataset in input_datasets.values():
            if not input_dataset.skip_publish:
                curr_data = input_dataset.curr_data.withColumn(
                    "etl_inserted", current_timestamp()
                ).withColumn("partition", lit(input_dataset.partition))
                if input_dataset.replace_partition:
                    curr_data.write.format("delta").mode("overwrite").option(
                        "replaceWhere",
                        f"partition = '{input_dataset.partition}'",
                    ).save(input_dataset.storage_path)
                else:
                    targetDF = DeltaTable.forPath(spark, input_dataset.storage_path)
                    (
                        targetDF.alias("target")
                        .merge(
                            curr_data.alias("source"),
                            self.construct_join_string(input_dataset.primary_keys),
                        )
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute()
                    )

    @abstractmethod
    def get_bronze_datasets(
        self, spark: SparkSession, **kwargs
    ) -> Dict[str, DeltaDataSet]:
        pass

    @abstractmethod
    def get_silver_datasets(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        spark: SparkSession,
        **kwargs,
    ) -> Dict[str, DeltaDataSet]:
        pass

    @abstractmethod
    def get_gold_datasets(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        spark: SparkSession,
        **kwargs,
    ) -> Dict[str, DeltaDataSet]:
        pass

    @log_metadata
    def run(self, spark: SparkSession, **kwargs):
        # add checks for presence of partition, pipeline_id and run_id
        partition = kwargs.get("partition")
        pipeline_id = kwargs.get("pipeline_id")
        run_id = kwargs.get("run_id")

        logging.info(
            f"Starting run process for pipeline_id: {pipeline_id}, run_id: {run_id}, partition: {partition}"
        )

        logging.info(
            f"Starting get_bronze_dataset for pipeline_id: {pipeline_id}, run_id: {run_id}, partition: {partition}"
        )
        bronze_data_sets = self.get_bronze_datasets(
            spark, partition=partition, run_id=run_id, pipeline_id=pipeline_id
        )
        self.validate_data(bronze_data_sets, run_id=run_id, pipeline_id=pipeline_id)
        self.publish_data(bronze_data_sets, spark)
        logging.info(
            "Created, validated & published bronze datasets:"
            f" {[ds for ds in bronze_data_sets.keys()]}"
        )

        logging.info(
            f"Starting get_silver_dataset for pipeline_id: {pipeline_id}, run_id: {run_id}, partition: {partition}"
        )
        silver_data_sets = self.get_silver_datasets(
            bronze_data_sets,
            spark,
            partition=partition,
            run_id=run_id,
            pipeline_id=pipeline_id,
        )
        self.validate_data(silver_data_sets, run_id=run_id, pipeline_id=pipeline_id)
        self.publish_data(silver_data_sets, spark)
        logging.info(
            "Created, validated & published silver datasets:"
            f" {[ds for ds in silver_data_sets.keys()]}"
        )

        logging.info(
            f"Starting get_gold_dataset for pipeline_id: {pipeline_id}, run_id: {run_id}, partition: {partition}"
        )
        gold_data_sets = self.get_gold_datasets(
            silver_data_sets,
            spark,
            partition=partition,
            run_id=run_id,
            pipeline_id=pipeline_id,
        )
        self.validate_data(gold_data_sets, run_id=run_id, pipeline_id=pipeline_id)
        self.publish_data(gold_data_sets, spark)
        logging.info(
            "Created, validated & published gold datasets:"
            f" {[ds for ds in gold_data_sets.keys()]}"
        )


class SalesMartETL(StandardETL):
    @log_metadata
    def get_bronze_datasets(
        self, spark: SparkSession, **kwargs
    ) -> Dict[str, DeltaDataSet]:
        customer_df, orders_df = generate_bronze_data(spark)
        return {
            "customer": DeltaDataSet(
                name="customer",
                curr_data=customer_df,
                primary_keys=["id", "partition"],
                storage_path=f"{self.STORAGE_PATH}/customer",
                table_name="customer",
                data_type="delta",
                database=f"{self.DATABASE}",
                partition=kwargs.get("partition", self.DEFAULT_PARTITION),
                replace_partition=True,
            ),
            "orders": DeltaDataSet(
                name="orders",
                curr_data=orders_df,
                primary_keys=["order_id", "partition"],
                storage_path=f"{self.STORAGE_PATH}/orders",
                table_name="orders",
                data_type="delta",
                database=f"{self.DATABASE}",
                partition=kwargs.get("partition", self.DEFAULT_PARTITION),
                replace_partition=True,
            ),
        }

    @log_metadata
    def get_dim_customer(
        self, customer: DeltaDataSet, spark: SparkSession, **kwargs
    ) -> DataFrame:
        customer_df = customer.curr_data
        dim_customer = kwargs["dim_customer"]
        # generete pk
        customer_df = customer_df.withColumn(
            "customer_sur_id",
            expr("md5(concat(id, datetime_updated))"),
        )
        # get only latest customer rows in dim_customer
        # since dim customer may have multiple rows per customer (SCD2)
        dim_customer_latest = dim_customer.where("current = true")

        # get net new rows to insert
        customer_df_insert_net_new = (
            customer_df.join(
                dim_customer_latest,
                (customer_df.id == dim_customer_latest.id)
                & (dim_customer_latest.datetime_updated < customer_df.datetime_updated),
                "leftanti",
            )
            .select(
                customer_df.id,
                customer_df.customer_sur_id,
                customer_df.first_name,
                customer_df.last_name,
                customer_df.state_id,
                customer_df.datetime_created,
                customer_df.datetime_updated,
            )
            .withColumn("current", lit(True))
            .withColumn("valid_from", customer_df.datetime_updated)
            .withColumn("valid_to", lit("2099-01-01 12:00:00.0000"))
        )

        # get rows to insert for existing ids
        customer_df_insert_existing_ids = (
            customer_df.join(
                dim_customer_latest,
                (customer_df.id == dim_customer_latest.id)
                & (dim_customer_latest.datetime_updated < customer_df.datetime_updated),
            )
            .select(
                customer_df.id,
                customer_df.customer_sur_id,
                customer_df.first_name,
                customer_df.last_name,
                customer_df.state_id,
                customer_df.datetime_created,
                customer_df.datetime_updated,
            )
            .withColumn("current", lit(True))
            .withColumn("valid_from", customer_df.datetime_updated)
            .withColumn("valid_to", lit("2099-01-01 12:00:00.0000"))
        )
        # get rows to be updated
        customer_df_ids_update = (
            dim_customer_latest.join(
                customer_df,
                (dim_customer_latest.id == customer_df.id)
                & (dim_customer_latest.datetime_updated < customer_df.datetime_updated),
            )
            .select(
                dim_customer_latest.id,
                dim_customer_latest.customer_sur_id,
                dim_customer_latest.first_name,
                dim_customer_latest.last_name,
                dim_customer_latest.state_id,
                dim_customer_latest.datetime_created,
                customer_df.datetime_updated,
                dim_customer_latest.valid_from,
            )
            .withColumn("current", lit(False))
            .withColumn("valid_to", customer_df.datetime_updated)
        )
        return customer_df_insert_net_new.unionByName(
            customer_df_insert_existing_ids
        ).unionByName(customer_df_ids_update)

    @log_metadata
    def get_fct_orders(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        spark: SparkSession,
        **kwargs,
    ) -> DataFrame:
        dim_customer = input_datasets["dim_customer"].curr_data
        orders_df = input_datasets["orders"].curr_data

        dim_customer_curr_df = dim_customer.where("current = true")
        return orders_df.join(
            dim_customer_curr_df,
            orders_df.customer_id == dim_customer_curr_df.id,
            "left",
        ).select(
            orders_df.order_id,
            orders_df.customer_id,
            orders_df.item_id,
            orders_df.item_name,
            orders_df.delivered_on,
            orders_df.datetime_order_placed,
            dim_customer_curr_df.customer_sur_id,
        )

    @log_metadata
    def get_silver_datasets(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        spark: SparkSession,
        **kwargs,
    ) -> Dict[str, DeltaDataSet]:
        self.check_required_inputs(input_datasets, ["customer", "orders"])
        dim_customer_df = self.get_dim_customer(
            input_datasets["customer"],
            spark,
            dim_customer=spark.read.table(f"{self.DATABASE}.dim_customer"),
        )

        silver_datasets = {}
        silver_datasets["dim_customer"] = DeltaDataSet(
            name="dim_customer",
            curr_data=dim_customer_df,
            primary_keys=["customer_sur_id"],
            storage_path=f"{self.STORAGE_PATH}/dim_customer",
            table_name="dim_customer",
            data_type="delta",
            database=f"{self.DATABASE}",
            partition=kwargs.get("partition", self.DEFAULT_PARTITION),
        )
        self.publish_data(silver_datasets, spark)
        silver_datasets["dim_customer"].curr_data = spark.read.table(
            f"{self.DATABASE}.dim_customer"
        )
        silver_datasets["dim_customer"].skip_publish = True
        input_datasets["dim_customer"] = silver_datasets["dim_customer"]

        silver_datasets["fct_orders"] = DeltaDataSet(
            name="fct_orders",
            curr_data=self.get_fct_orders(input_datasets, spark),
            primary_keys=["order_id"],
            storage_path=f"{self.STORAGE_PATH}/fct_orders",
            table_name="fct_orders",
            data_type="delta",
            database=f"{self.DATABASE}",
            partition=kwargs.get("partition", self.DEFAULT_PARTITION),
            replace_partition=True,
        )
        return silver_datasets

    @log_metadata
    def get_sales_mart(
        self, input_datasets: Dict[str, DeltaDataSet], **kwargs
    ) -> DataFrame:
        dim_customer = (
            input_datasets["dim_customer"]
            .curr_data.where("current = true")
            .select("customer_sur_id", "state_id")
        )
        fct_orders = input_datasets["fct_orders"].curr_data
        return (
            fct_orders.alias("fct_orders")
            .join(
                dim_customer.alias("dim_customer"),
                fct_orders.customer_sur_id == dim_customer.customer_sur_id,
                "left",
            )
            .select(
                expr('to_date(fct_orders.delivered_on, "yyyy-dd-mm")').alias(
                    "deliver_date"
                ),
                col("dim_customer.state_id").alias("state_id"),
            )
            .groupBy("deliver_date", "state_id")
            .count()
            .withColumnRenamed("count", "num_orders")
        )

    @log_metadata
    def get_gold_datasets(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        spark: SparkSession,
        **kwargs,
    ) -> Dict[str, DeltaDataSet]:
        self.check_required_inputs(input_datasets, ["dim_customer", "fct_orders"])
        sales_mart_df = self.get_sales_mart(input_datasets)
        return {
            "sales_mart": DeltaDataSet(
                name="sales_mart",
                curr_data=sales_mart_df,
                primary_keys=["deliver_date", "state_id", "partition"],
                storage_path=f"{self.STORAGE_PATH}/sales_mart",
                table_name="sales_mart",
                data_type="delta",
                database=f"{self.DATABASE}",
                partition=kwargs.get("partition", self.DEFAULT_PARTITION),
                replace_partition=True,
            )
        }


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("adventureworks").enableHiveSupport().getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    log4j_logger = spark._jvm.org.apache.log4j  # noqa
    spark_logger = log4j_logger.LogManager.getLogger("Custom Logger")
    spark_logger.debug("some debugging message")
    spark_logger.info("some info message")
    spark_logger.warn("some warning message")
    spark_logger.error("some error message")
    spark_logger.fatal("some fatal message")

    # https://polarpersonal.medium.com/writing-pyspark-logs-in-apache-spark-and-databricks-8590c28d1d51 for logging
    sm = SalesMartETL()
    partition = datetime.now().strftime(
        "%Y-%m-%d-%H-%M-%S"
    )  # usually from orchestrator -%S
    pipeline_id = "sales_mart"
    run_id = f"{pipeline_id}_{partition}"
    sm.run(spark, partition=partition, run_id=run_id, pipeline_id=pipeline_id)
    spark.stop
