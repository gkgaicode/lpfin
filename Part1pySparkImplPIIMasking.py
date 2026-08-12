from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, sha2, concat_ws, lit
from pyspark.sql.types import StringType
import boto3
import base64

# 1. Initialize Spark Session optimized for streaming ingestion
spark = SparkSession.builder \
    .appName("LPL_Financial_Inbound_PII_Masking") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

# 2. Setup AWS KMS client for cryptographic operations (Executed per worker partition)
def encrypt_field_kms(payload: str) -> str:
    if not payload:
        return ""
    try:
        # Initialised inside the partition mapping execution loop to avoid serialization errors
        kms_client = boto3.client('kms', region_name='us-east-1')
        response = kms_client.encrypt(
            KeyId='arn:aws:kms:us-east-1:123456789012:key/your-custom-cmk-id',
            Plaintext=payload.encode('utf-8')
        )
        return base64.b64encode(response['CiphertextBlob']).decode('utf-8')
    except Exception as e:
        # Ensure poison pills are flagged for routing to the SQS Dead Letter Queue
        return f"ERROR_DLQ: {str(e)}"

# Register the cryptographic method as a PySpark User Defined Function (UDF)
encrypt_kms_udf = udf(encrypt_field_kms, StringType())

# 3. Simulate reading the streaming DataFrame from Amazon MSK
# In production, use: spark.readStream.format("kafka").option("kafka.bootstrap.servers", "host:port").load()
raw_stream_df = spark.readStream.format("rate").option("rowsPerSecond", "50000").load()

# Let's assume incoming_df contains columns: ['account_id', 'ssn', 'client_name', 'trade_amount']
# For demonstration, we simulate data fields onto the rate framework stream
incoming_df = raw_stream_df \
    .withColumn("account_id", col("value").cast(StringType())) \
    .withColumn("ssn", lit("123-456-7890")) \
    .withColumn("client_name", lit("John Doe")) \
    .withColumn("trade_amount", col("value") * 1.5)

# 4. Execute the Perimeter Masking Strategy
# - SSN: Encrypted via AWS KMS (retains capability for downstream authorized decryption)
# - Client Name: Tokenized/Hashed via SHA-256 with a secure corporate salt
salt_value = "LPL_Sec_Salt_2026_"

secured_stream_df = incoming_df \
    .withColumn("encrypted_ssn", encrypt_kms_udf(col("ssn"))) \
    .withColumn("hashed_client_name", sha2(concat_ws("", col("client_name"), lit(salt_value)), 256)) \
    .drop("ssn", "client_name")  # Drops raw unencrypted PII prior to writing to S3 staging

# 5. Route records that fail masking to a DLQ trigger column, stream the clean ones to S3 Staging
validated_stream_df = secured_stream_df.withColumn(
    "is_corrupted", 
    when(col("encrypted_ssn").startswith("ERROR_DLQ"), True).otherwise(False)
)

# 6. Stream processed data to the Amazon S3 Lake Target (Parquet format, partitioned for Snowflake)
query = validated_stream_df \
    .writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "s3a://lpl-data-lake-gold/transactions/") \
    .option("checkpointLocation", "s3a://lpl-data-lake-gold/checkpoints/") \
    .partitionBy("is_corrupted") \
    .trigger(processingTime='15 minutes') \
    .start()

query.awaitTermination()
