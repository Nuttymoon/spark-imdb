package tsvtohive

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, regexp_replace, to_date, udf, concat_ws,
  sum, collect_list, when, first, concat, lit}

object HiveImport {
  /**
    * Replace a string by None if their is no value (-> null in Spark)
    * @param str
    * @return
    */
  def catchNull(str: String): Option[String] = {
    val s: String = Option(str).getOrElse(return None)
    if (s == "\\N" || s == "") {
      None
    } else {
      Some(s)
    }
  }

  val replaceNull = udf[Option[String], String](catchNull)

  def main(args: Array[String]): Unit ={
    val spark = SparkSession
      .builder
      .appName("TSV to Hive")
      .config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .config("hive.metastore.uris", "thrift://smaster.sparkcl.local:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    val dbName = "imdb"

    sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    namesToHive(spark, dbName)
    titlesVideosToHive(spark, dbName)
  }

  def namesToHive(spark: SparkSession, dbName: String): Unit ={
    import spark.sql

    val namesColumns = StructType(
      StructField("id", StringType) ::
        StructField("name", StringType) ::
        StructField("birth_year", StringType) ::
        StructField("death_year", StringType) ::
        StructField("professions", StringType) ::
        StructField("famous_titles", StringType) :: Nil)

    val names = spark
      .read
      .option("sep", "\t")
      .schema(namesColumns)
      .option("header", "true")
      .csv("/user/spark/spark-training/imdb/name.basics.tsv")

      .withColumn("id",
        regexp_replace(col("id"), "nm", ""))
      .withColumn("birth_year",
        to_date(col("birth_year"), "yyyy").cast(DateType))
      .withColumn("death_year",
        to_date(col("death_year"), "yyyy").cast(DateType))
      .withColumn("professions", replaceNull(col("professions")))
      .withColumn("famous_titles",
        regexp_replace(
          replaceNull(col("famous_titles")), "tt", ""))

    // Create table imdb.names
    sql("CREATE TABLE IF NOT EXISTS " +
      s"$dbName.names(id STRING, name STRING, birth_year DATE, " +
      "death_year DATE, professions STRING, famous_titles STRING)")

    // Write to Hive
    names.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("imdb.names")
  }

  def titlesVideosToHive(spark: SparkSession, dbName: String): Unit ={
    import spark.sql

    val titleColumns = StructType(
      StructField("video_id", StringType) ::
        StructField("title_number", IntegerType) ::
        StructField("title", StringType) ::
        StructField("region", StringType) ::
        StructField("language", StringType) ::
        StructField("types", StringType) ::
        StructField("attributes", StringType) ::
        StructField("original", StringType) :: Nil)

    val titles = spark
      .read
      .option("sep", "\t")
      .schema(titleColumns)
      .option("header", "true")
      .csv("/user/zeppelin/spark-training/imdb/title.akas.tsv")

      .withColumn("video_id", regexp_replace(col("video_id"), "tt", ""))
      .withColumn("title_number", col("title_number").cast(IntegerType))
      .withColumn("region", replaceNull(col("region")))
      .withColumn("language", replaceNull(col("language")))
      .withColumn("types", replaceNull(col("types")))
      .withColumn("attributes", replaceNull(col("attributes")))
      .withColumn("original", replaceNull(col("original")).cast(BooleanType))

    val videoColumns = StructType(
      StructField("id", StringType) ::
        StructField("type", StringType) ::
        StructField("primary_title", StringType) ::
        StructField("original_title", StringType) ::
        StructField("adult", StringType) ::
        StructField("start_year", StringType) ::
        StructField("end_year", StringType) ::
        StructField("minutes", StringType) ::
        StructField("genres", StringType) :: Nil)

    val videos = spark
      .read
      .option("sep", "\t")
      .schema(videoColumns)
      .option("header", "true")
      .csv("/user/zeppelin/spark-training/imdb/title.basics.tsv")
      .withColumn("id", regexp_replace(col("id"), "tt", ""))
      .withColumn("primary_title", replaceNull(col("primary_title")))
      .withColumn("original_title", replaceNull(col("original_title")))
      .withColumn("adult", col("adult").cast(BooleanType))
      .withColumn("start_year", col("start_year").cast(DateType))
      .withColumn("end_year", col("end_year").cast(DateType))
      .withColumn("minutes", col("minutes").cast(IntegerType))

    // Find the original title for videos that don't have one
    val toCorrectOriginals = titles
      .withColumn("original", col("original").cast(IntegerType))
      .groupBy("video_id")
      .agg(concat_ws(",",
        collect_list("types")).as("types"),
        sum("original").as("original"))
      .where(!col("types").contains("original") || col("original") === 0)
      .withColumnRenamed("video_id", "id")
      .join(videos.select("id", "original_title"), "id")
      .select("id", "original_title")

    val correctedTitles = titles

      // Add original titles when possible
      .join(toCorrectOriginals,
        toCorrectOriginals("id") === titles("video_id"),
        "outer")
      .withColumn("types",
        when(col("title") === col("original_title") && !col("types").isNull,
          concat(col("types"), lit(",original")))
        .when(col("title") === col("original_title") && col("types").isNull,
          lit("original"))
        .otherwise(col("types")))
      .withColumn("original",
        when(col("title") === col("original_title"), true)
        .otherwise(col("original")))
      .select("video_id", "title", "title_number", "region", "language", "types", "attributes", "original")

    // Create table imdb.videos
    sql("CREATE TABLE IF NOT EXISTS " +
      s"$dbName.videos(id STRING, primary_title STRING, original_title STRING, adult BOOLEAN, " +
      "start_year DATE, end_year DATE, minutes INT)")

    // Create table imdb.titles
    sql("CREATE TABLE IF NOT EXISTS " +
      s"$dbName.titles(video_id STRING, title STRING, title_number INT, " +
      "language STRING, types STRING, attributes STRING, original BOOLEAN) " +
      "PARTITIONED BY(region STRING)")

    // Write to Hive
    videos
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("imdb.videos")

    correctedTitles
      .write
      .partitionBy("region")
      .mode(SaveMode.Overwrite)
      .saveAsTable("imdb.titles")
  }
}
