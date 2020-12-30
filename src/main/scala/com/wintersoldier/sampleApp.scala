package com.wintersoldier
import org.apache.spark.sql.{DataFrame, SparkSession}

object sampleApp
{
    val spark: SparkSession = SparkSession
        .builder()
        .appName("sampleApp")
        .master("local[*]")
        .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setCheckpointDir("/home/wintersoldier/Desktop/checkpoint")
    
    spark.sparkContext.setLogLevel("ERROR")
    def main(array: Array[String]): Unit ={
        
        val brokerUrl_ : String = "tcp://localhost:61616"
        val topicName_ : String = "sample_topic3"
        val username_ : String = "username"
        val password_ : String = "password"
        val connectionType : String = "activemq"
        val clientId = "coldplay"
        val acknowledge = "true"
        
        val df = spark
            .readStream
            .format("com.wintersoldier.linkinJMS")
            .option("connection", connectionType)
            .option("brokerUrl", brokerUrl_)
            .option("topic", topicName_)
            .option("username", username_)
            .option("password", password_)
            .option("acknowledge", acknowledge)
            .option("clientId", clientId)
            .load()
    
        df.writeStream
            .outputMode("append")
            .format("console")
            .option("checkpointLocation", "/home/wintersoldier/Desktop/checkpoint")
            .start
            .awaitTermination()
        spark.close()
        
    }
}
