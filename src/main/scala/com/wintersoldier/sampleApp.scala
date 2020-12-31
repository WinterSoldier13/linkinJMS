package com.wintersoldier

import org.apache.spark.sql.{DataFrame, SparkSession}

object sampleApp {
    val spark: SparkSession = SparkSession
        .builder()
        .appName("sampleApp")
        .master("local[*]")
        .getOrCreate()
    
    import spark.implicits._
    
    spark.sparkContext.setCheckpointDir("/home/wintersoldier/Desktop/checkpoint")
    
    spark.sparkContext.setLogLevel("ERROR")
    
    def main(array: Array[String]): Unit = {
        
        val brokerUrl : String = "tcp://localhost:61616"
        val topicName : String = "sample_topic3"
        val username : String = "username"
        val password : String = "password"
        val connectionType: String = "activemq"
        val clientId : String = "coldplay"
        val acknowledge: String = "true"
        val readInterval: String = "1000"
        
        val df = spark
            .readStream
            .format("com.wintersoldier.linkinJMS")
            .option("connection", connectionType)
            .option("brokerUrl", brokerUrl)
            .option("topic", topicName)
            .option("username", username)
            .option("password", password)
            .option("acknowledge", acknowledge)
            .option("clientId", clientId)
            .option("readInterval", readInterval)
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
