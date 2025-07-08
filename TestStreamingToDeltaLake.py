from pyspark.sql import SparkSession
from datetime import datetime
from StreamingToDeltaLake import StreamingToDeltaLake
from dotenv import dotenv_values
from lib.logger import Log4j

app_name = "TestStreamingToDeltaLake"
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
application_config = dotenv_values(".env")
aws_access_key = application_config["AWS_ACCESS_KEY"]
aws_secret_key = application_config["AWS_SECRET_KEY"]
spark = SparkSession \
            .builder \
            .appName(app_name) \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
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
                    f"-Dlogfile.name={app_name}-{timestamp}") \
            .master("local[2]") \
            .getOrCreate()
logger = Log4j(spark)

class TestStreamingToDeltaLake:

    def assertResult(self, expected_count):
        logger.info(f"\tStarting validation...")
        actual_count = spark.sql("select count(*) from invoices_bz").collect()[0][0]
        assert expected_count == actual_count, f"Test failed! actual count is {actual_count}"
        logger.info("Done")

    def waitForMicroBatch(self, sleep=15):
        import time
        logger.info(f"\tWaiting for {sleep} seconds...")
        time.sleep(sleep)
        logger.info("Done.")

    def runTests(self):
        s = StreamingToDeltaLake()
        logger.info("Testing Scenario - Start from beginning on a new checkpoint...")
        bzQuery = s.process()
        self.waitForMicroBatch()
        bzQuery.stop()
        self.assertResult(1590)
        logger.info("Validation passed.\n")

if __name__ == "__main__":
    streaming = TestStreamingToDeltaLake()
    streaming.runTests()