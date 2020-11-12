import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import java.time._

import CONSTANTS._


object LegoChainBuilder {
  def main(args : Array[String]) : Unit = {
    val spark = SparkSession.builder.appName("Lego ChainBuilder").getOrCreate()
    import spark.implicits._

    val channel_creator_udf = spark.udf.register("channel_creator_udf", channel_creator())
    val touch_creator_udf = spark.udf.register("touch_creator_udf", touch_creator_udf(_:String,_:Long,_:String))
    val recursion_chain_udf = spark.udf.register("recursion_chain_udf",recursion_chain[Touch])

    val optionsMap  = argsPars(args, "j") //Parse input arguments from command line
    val validMap    = argsValid(optionsMap) // Validation input arguments types and building Map

    val dates_window = DatesWindow(validMap("date_tHOLD").head.toString,
                                   validMap("date_start").head.toString,
                                   validMap("date_finish").head.toString)

    val(date_tHOLDValid,date_startValid,date_finishValid) = dates_window.correct_chronology match {
      case true => (dates_window.get_tHOLD,dates_window.get_start,dates_window.get_finish)
      case false => throw new Exception("problem with DATES !!!!!")
    }

    val data = spark.read.
      format("parquet").
      option("inferSchema","false").
      option("mergeSchema","true").
      load(validMap("flat_path").map(_.toString):_*)

    val data_work = data.select(
      $"ProjectID".cast(sql.types.LongType),
      $"ClientID".cast(sql.types.StringType),
      $"HitTimeStamp".cast(sql.types.LongType),
      $"ga_sourcemedium".cast(sql.types.StringType),
      $"utm_source".cast(sql.types.StringType),
      $"utm_medium".cast(sql.types.StringType),
      $"utm_campaign".cast(sql.types.StringType),
      $"interaction_type".cast(sql.types.StringType),
      $"profile".cast(sql.types.StringType),
      $"ga_location".cast(sql.types.StringType),
      $"goal".cast(sql.types.LongType),
      $"src".cast(sql.types.StringType),
      $"sessionID".cast(sql.types.StringType),
      $"transactionID".cast(sql.types.StringType)
    )

    val data_custom = data_work.
      filter($"HitTimeStamp" >= date_tHOLDValid && $"HitTimeStamp" < date_finishValid).
      filter($"ProjectID".isin(validMap("projectID"):_*)).
      filter($"goal".isNull || $"goal".isin(validMap("target_numbers"):_*)).
      filter($"src".isin(validMap("source_platform"):_*)).
      filter{validMap("product_name") match {
        case productList @ x :: tail => $"ga_location".isin(productList:_*)
        case _ => $"ga_location"
      }}

    val data_preprocess_0 = data_custom.withColumn("channel",channel_creator_udf($"src",
      $"ga_sourcemedium",
      $"utm_source",
      $"utm_medium",
      $"utm_campaign",
      $"interaction_type",
      $"profileID",
      $"sessionID",
      $"transactionID" )).select(
      $"ClientID",
      $"HitTimeStamp",
      $"goal",
      $"channel"
    )

    val data_preprocess_1 = data_preprocess_0.withColumn("conversion",
      when($"goal".isin(validMap("target_numbers"):_*),CONVERSION_SYMBOL).otherwise(NO_CONVERSION_SYMBOL)).
      select($"ClientID",
      $"HitTimeStamp",
      $"conversion",
      $"channel").sort($"ClientID", $"HitTimeStamp".asc).
      cache()

    val actorsID = data_preprocess_1.
      filter($"HitTimeStamp" >= date_startValid && $"HitTimeStamp" < date_finishValid).
      filter($"conversion" === CONVERSION_SYMBOL).
      select($"ClientID").
      distinct()

    val data_bulk = validMap("achieve_mode").head match {

      case true => data_preprocess_1.as("df1").
        join(actorsID.as("df2"),($"df1.ClientID" === $"df2.ClientID"),"inner").
        select($"df1.*")

      case false => {val allID = data_preprocess_1.select($"ClientID").distinct()
        val notConvertedID = allID.except(actorsID)
        data_preprocess_1.as("df1").
          join(notConvertedID.as("df2"),($"df1.ClientID" === $"df2.ClientID"),"inner").
          select($"df1.*")
      }
    }

    val data_touch = data_bulk.withColumn("touch",touch_creator_udf($"channel",$"HitTimestamp",$"conversion")).
      select($"ClientID",
      $"touch")


    val data_group = data_touch.groupBy($"ClientID").agg(collect_list($"touch").as("touch_list"))

    val data_touchTube = data_group.select($"ClientID",recursion_chain_udf($"touch_list")($"touch_list"(2) == CONVERSION_SYMBOL))
















  }

}
