package africa.absa
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * @author ${user.name}
 */
object App {
  

  
  def main(args : Array[String]) {
    val mongohost = "22.240.124.23"
    val mongoport = "27017"
    val collectionname = "test"
    val dbname = "client"
    val mongoURI = s"mongodb://$mongohost:$mongoport/$dbname.$collectionname"
    val cepath = s"/bigdatahdfs/project/c1v/absa/complex_entity"
    val personpath = s"/bigdatahdfs/project/c1v/absa/person"
    val sidpath = s"/bigdatahdfs/project/c1v/absa/source_identifier"

    val spark = SparkSession.builder().master("local").config("spark.driver.bindAddress", "127.0.0.1").config("spark.mongodb.output.uri", mongoURI).getOrCreate()
    import spark.implicits._

    import com.mongodb.spark._
//    val data = spark.sparkContext.parallelize(Seq((1,2),(3,4))).toDF()

    //inputs
    val reportDate = "2020-05-05"
    val sourceColumn = Seq("SDS","CIF")
        //getting complex entity data
    val dt = reportDate
    val source = sourceColumn


    val ce = spark.read.parquet(cepath).filter($"report_date"===dt).select($"single_unique_party_id".as("party_suins"))

    val cesid = spark.read.parquet(sidpath)
      .filter($"report_date"===dt)
      .join(ce,$"single_unique_party_id"===$"party_suins","inner")
      .filter($"expiry_date".isNull && $"source_system".isin(source:_*))


    //complex entity duplicates
    val ceduplicates = cesid.groupBy($"system_id",$"source_system").agg(collect_set($"single_unique_party_id").as("suins"))
      .filter(size($"suins")>1)
      .select($"system_id".as("unique_id"),$"source_system",$"suins".as("assoicated_ids"))
      .withColumn("party",lit("complexEntity"))
      .withColumn("issue",lit("duplicate"))

    //complex entity unmerges
    val ceunmerge = cesid.groupBy($"single_unique_party_id",$"source_system").agg(collect_set($"system_id").as("system_ids")).filter(size($"system_ids")>1).
      select($"single_unique_party_id".as("unique_id"),$"source_system",$"system_ids".as("assoicated_ids")).withColumn("party",lit("complexEntity")).withColumn("issue",lit("unmerge"))
    //complex entity result
    val ce = ceduplicates.union(ceunmerge)

    MongoSpark.save(data.write.option("collection", collectionname).mode("overwrite"))









    // Person Dupicates and Unmerge
    val party_type = "person"
    val party = if (party_type == "ce") {spark.read.parquet("/bigdatahdfs/project/c1v/absa/complex_entity").
                    filter($"report_date"===dt).select($"single_unique_party_id".as("party_suins"))}
                else {spark.read.parquet("/bigdatahdfs/project/c1v/absa/person").
                    filter($"report_date"===dt).select($"single_unique_party_id".as("party_suins"))}

    val sid = spark.read.parquet("/bigdatahdfs/project/c1v/absa/source_identifier").
      filter($"report_date"===dt).join(party,$"single_unique_party_id"===$"party_suins","inner").
      filter($"expiry_date".isNull && $"source_system".isin(source:_*))
    //person duplicates
    val personduplicates = sid.groupBy($"system_id",$"source_system").agg(collect_set($"single_unique_party_id").as("suins")).filter(size($"suins")>1).
      select($"system_id".as("unique_id"),$"source_system",$"suins".as("assoicated_ids")).withColumn("party",lit("complexEntity")).withColumn("issue",lit("duplicate"))
    //person unmerges
    val personunmerge = sid.groupBy($"single_unique_party_id",$"source_system").agg(collect_set($"system_id").as("system_ids")).filter(size($"system_ids")>1).
      select($"single_unique_party_id".as("unique_id"),$"source_system",$"system_ids".as("assoicated_ids")).withColumn("party",lit("complexEntity")).withColumn("issue",lit("unmerge"))
    //person result
    val person = personduplicates.union(personunmerge)
    //complex entity and person merge
    val issueList = ce.union(person)


  }

}
