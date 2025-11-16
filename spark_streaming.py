# spark_streaming.py (robust final version)
import os
import glob
import logging
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import (
    col, expr, udf, array_max, when, size as spark_size, lit
)
from pyspark.sql.types import StructType, StringType, DoubleType, ArrayType
from pyspark.sql.utils import AnalysisException

# -------------------- logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("news-sentiment-stream")

# -------------------- config -----------------------
MODEL_DIR = "models/news_sentiment_pipeline"
IN_DIR = "data/incoming"
OUT_DIR = "data/predictions"
CHECKPOINT_DIR = "checkpoints/news_stream"

os.makedirs(IN_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(CHECKPOINT_DIR) or ".", exist_ok=True)

# -------------------- fallback UDFs ----------------
def max_prob(v):
    try:
        return float(max(v)) if v else 0.0
    except Exception:
        return 0.0

max_prob_udf = udf(max_prob, DoubleType())

def vector_to_list(v):
    try:
        return v.toArray().tolist()
    except Exception:
        return []

vector_to_list_udf = udf(vector_to_list, ArrayType(DoubleType()))

# -------------------- main ------------------------
def main():
    spark = SparkSession.builder \
        .appName("news-sentiment-stream") \
        .master("local[*]") \
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .getOrCreate()

    logger.info("Spark session created.")

    # ------------------ robust schema detection / fallback ------------------
    explicit_schema = StructType() \
        .add("id", StringType()) \
        .add("text", StringType()) \
        .add("source", StringType()) \
        .add("url", StringType()) \
        .add("timestamp", StringType())

    try:
        files = glob.glob(os.path.join(IN_DIR, "*.json"))
        if files:
            # read a few files to infer schema
            static_df = spark.read.option("multiline", "false").json(files[:10])
            schema_to_use = static_df.schema
            logger.info("Inferred schema from sample files.")
        else:
            schema_to_use = explicit_schema
            logger.info("No sample files present; using explicit schema.")
        # use maxFilesPerTrigger to avoid spikes when many files appear
        streaming_df = spark.readStream.schema(schema_to_use).option("maxFilesPerTrigger", 1).json(IN_DIR)
    except AnalysisException as ae:
        logger.warning("AnalysisException while reading static JSON: %s", ae)
        streaming_df = spark.readStream.schema(explicit_schema).option("maxFilesPerTrigger", 1).json(IN_DIR)
    except Exception as e:
        logger.warning("Warning: fallback to explicit schema: %s", e)
        streaming_df = spark.readStream.schema(explicit_schema).option("maxFilesPerTrigger", 1).json(IN_DIR)
    # -----------------------------------------------------------------------

    # ------------------ load model ------------------
    try:
        model = PipelineModel.load(MODEL_DIR)
        logger.info("Loaded model from %s", MODEL_DIR)
    except Exception as e:
        logger.error("Failed to load model from '%s': %s", MODEL_DIR, e)
        logger.error("Run 'python train_model.py' to create the model, then restart this script.")
        spark.stop()
        return
    # -------------------------------------------------

    # Apply model (PipelineModel is a Transformer)
    transformed = model.transform(streaming_df)

    # ------------------ probability handling ------------------
    # Try to use vector_to_array (Spark 3+) for performance, else fallback to Python UDF
    try:
        from pyspark.ml.functions import vector_to_array  # type: ignore
        prob_array_col = vector_to_array(col("probability"))
        use_array_max = True
        logger.info("Using vector_to_array for probability -> array conversion.")
    except Exception:
        prob_array_col = vector_to_list_udf(col("probability"))
        use_array_max = False
        logger.warning("vector_to_array not available — using python UDF fallback (vector_to_list_udf).")

    # If the model didn't produce 'probability', add a default [0.0, 0.0] to be safe
    if "probability" not in transformed.columns:
        logger.warning("'probability' column not found in model output. Adding default [0.0, 0.0].")
        transformed = transformed.withColumn("probability", lit([0.0, 0.0]))

    # Build result with robust indexing and safe defaults (avoid OOB)
    result = transformed.withColumn("prob_values", prob_array_col)

    result = result.withColumn(
        "prob_negative",
        when(
            (col("prob_values").isNotNull()) & (spark_size(col("prob_values")) >= 1),
            col("prob_values").getItem(0)
        ).otherwise(0.0)
    ).withColumn(
        "prob_positive",
        when(
            (col("prob_values").isNotNull()) & (spark_size(col("prob_values")) >= 2),
            col("prob_values").getItem(1)
        ).otherwise(0.0)
    )

    # confidence: prefer array_max (fast) else fall back to UDF
    if use_array_max:
        result = result.withColumn("confidence", array_max(col("prob_values")))
    else:
        result = result.withColumn("confidence", max_prob_udf(col("prob_values")))

    # sentiment mapping — ensure your training used 1.0 as Positive
    result = result.withColumn("sentiment", expr("CASE WHEN prediction = 1.0 THEN 'Positive' ELSE 'Negative' END"))

    # drop helper column to keep output tidy
    result = result.drop("prob_values")
    # -----------------------------------------------------------------------

    # ------------------ write stream ------------------
    checkpoint_path = os.path.abspath(CHECKPOINT_DIR)
    logger.info("Starting streaming: watching %s -> writing to %s (checkpoint=%s)", IN_DIR, OUT_DIR, checkpoint_path)

    query = result.select(
        "id", "text", "source", "url", "timestamp", "sentiment", "prob_negative", "prob_positive", "confidence"
    ).writeStream \
        .format("json") \
        .option("path", OUT_DIR) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping streaming due to KeyboardInterrupt.")
        query.stop()
    except Exception as ex:
        logger.error("Streaming error: %s", ex)
        query.stop()
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
