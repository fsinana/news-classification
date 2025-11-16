# train_model.py
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import PipelineModel
import os

MODEL_DIR = "models/news_sentiment_pipeline"
DATA_CSV = "data/labeled_news.csv"  # expected columns: text,label

def create_sample_csv():
    import pandas as pd
    sample = [
        ("Stocks surge after positive earnings report", 1),
        ("Company recalls product after safety concerns", 0),
        ("New AI breakthrough promises energy savings", 1),
        ("Executive arrested in fraud scandal", 0),
        ("Local startup raises seed round", 1),
        ("Data breach exposes user records", 0)
    ]
    df = pd.DataFrame(sample, columns=["text","label"])
    os.makedirs("data", exist_ok=True)
    df.to_csv(DATA_CSV, index=False)
    print(f"Sample labeled CSV created at {DATA_CSV}")

def main():
    spark = SparkSession.builder \
        .appName("train-news-sentiment") \
        .master("local[*]") \
        .getOrCreate()

    if not os.path.exists(DATA_CSV):
        create_sample_csv()

    df = spark.read.csv(DATA_CSV, header=True, inferSchema=True).select("text", "label")
    # make sure label is numeric:
    df = df.withColumn("label", df["label"].cast("double"))

    tokenizer = RegexTokenizer(inputCol="text", outputCol="tokens", pattern="\\W")
    remover = StopWordsRemover(inputCol="tokens", outputCol="filtered")
    hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1<<16)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)

    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])

    model = pipeline.fit(df)
    # Save the pipeline model
    model.write().overwrite().save(MODEL_DIR)
    print(f"Model saved to {MODEL_DIR}")

    spark.stop()

if __name__ == "__main__":
    main()
