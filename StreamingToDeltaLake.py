from pyspark.sql import SparkSession
from lib.logger import Log4j
from datetime import datetime
from dotenv import dotenv_values

# PYSPARK_SUBMIT_ARGS --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.kafka:kafka-clients:3.5.1 pyspark-shell
# STATIC. Initiate a spark session.

class StreamingToDeltaLake:
    def __init__(self):
        self.checkpointing_dir = "checkpointing"
        self.application_config = dotenv_values(".env")
        self.aws_access_key = self.application_config["AWS_ACCESS_KEY"]
        self.aws_secret_key = self.application_config["AWS_SECRET_KEY"]
        self.delta_table_s3_location = "s3a://deltalake-streaming/delta/invoices_bz/"
        self.schema = """InvoiceNumber string, CreatedTime bigint, StoreID string, PosID string, CashierID string,
                CustomerType string, CustomerCardNo string, TotalAmount double, NumberOfItems bigint, 
                PaymentMethod string, TaxableAmount double, CGST double, SGST double, CESS double, 
                DeliveryType string,
                DeliveryAddress struct<AddressLine string, City string, ContactNumber string, PinCode string, 
                State string>,
                InvoiceLineItems array<struct<ItemCode string, ItemDescription string, 
                    ItemPrice double, ItemQty bigint, TotalValue double>>
        """
        self.app_name = "StreamingToDeltaLake"
        self.timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.spark = SparkSession \
            .builder \
            .appName(self.app_name) \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.hadoop.fs.s3a.access.key", self.aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.aws_secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,"
                    "org.apache.kafka:kafka-clients:3.5.1,"
                    "io.delta:delta-spark_2.12:3.1.0,"
                    "org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.driver.extraJavaOptions",
                    "-Dlog4j.configuration=file:log4j.properties "
                    "-Dspark.yarn.app.container.log.dir=app-logs "
                    f"-Dlogfile.name={self.app_name}-{self.timestamp}") \
            .master("local[2]") \
            .getOrCreate()
        self.logger = Log4j(self.spark)

    def raw_read_from_kafka(self, startTime = 1):
        return ( self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.application_config["bootstrap.servers"])
            .option("kafka.security.protocol", self.application_config["security.protocol"])
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", f"{self.application_config['jass.module']} required username='{self.application_config['sasl.username']}' password='{self.application_config['sasl.password']}';")
            .option("subscribe", "invoices")
            .option("maxOffsetsPerTrigger", 1000)
            .option("startingTimestamp", startTime) # This is a one-time effect — it only applies when there's no existing checkpoint (first run).
            .load()
        )

    def getInvoices(self, kafka_df):
        from pyspark.sql.functions import cast, from_json
        return (kafka_df.select(from_json(kafka_df.value.cast("string"), self.schema).alias("value")).select("value.*")
        )

    def deltaTableWrite(self, df):
        #Create table if not exist
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS invoices_bz (
              InvoiceNumber STRING,
              CreatedTime BIGINT,
              StoreID STRING,
              PosID STRING,
              CashierID STRING,
              CustomerType STRING,
              CustomerCardNo STRING,
              TotalAmount DOUBLE,
              NumberOfItems BIGINT,
              PaymentMethod STRING,
              TaxableAmount DOUBLE,
              CGST DOUBLE,
              SGST DOUBLE,
              CESS DOUBLE,
              DeliveryType STRING,
              DeliveryAddress STRUCT<
                AddressLine: STRING,
                City: STRING,
                ContactNumber: STRING,
                PinCode: STRING,
                State: STRING
              >,
              InvoiceLineItems ARRAY<STRUCT<
                ItemCode: STRING,
                ItemDescription: STRING,
                ItemPrice: DOUBLE,
                ItemQty: BIGINT,
                TotalValue: DOUBLE
              >>
            )
            USING DELTA
            LOCATION '{self.delta_table_s3_location}';
        """)

        sQuery =  ( df.writeStream
                        .format("delta")
                        .queryName("bronze-ingestion")
                        .option("checkpointLocation", self.checkpointing_dir)
                        .outputMode("append")
                        .toTable("invoices_bz")
                    )
        return sQuery

    def process(self, startTime = 1):
        self.logger.info("Start reading data from kafka")
        kafka_ingest_df = self.raw_read_from_kafka(startTime)
        self.logger.info("Reading data from kafka Done")
        self.logger.info("Flatten the data")
        invoices_df = self.getInvoices(kafka_ingest_df)
        self.logger.info("Flattening of data Done.")
        return self.deltaTableWrite(invoices_df)