package com.wintersoldier

import org.apache.spark.sql.{DataFrame, SparkSession}

object sampleApp
{
    implicit val spark: SparkSession = SparkSession
        .builder()
        .appName("sampleApp")
        .master("local[2]")
        .getOrCreate()
    
    import spark.implicits._
    
//    spark.sparkContext.setCheckpointDir("/home/wintersoldier/Desktop/checkpoint")
    spark.sparkContext.setLogLevel("ERROR")
    
    
    def main(array: Array[String]): Unit = {
        
        val brokerUrl: String = "tcp://localhost:61616"
        val topicName: String = "sample_topic3"
        val username: String = "username"
        val password: String = "password"
        val connectionType: String = "activemq"
        val clientId: String = "coldplay"
        val acknowledge: String = "false"
        val readInterval: String = "2000"
        val queueName : String = "sampleQ"
        
        implicit val df: DataFrame = spark
            .readStream
            .format("com.wintersoldier.linkinJMS")
            .option("connection", connectionType)
            .option("brokerUrl", brokerUrl)
//            .option("topic", topicName)
            .option("queue", queueName)
            .option("username", username)
            .option("password", password)
            .option("acknowledge", acknowledge)
            .option("clientId", clientId)
            .option("readInterval", readInterval)
            .load()
        
        
        df.writeStream
//            .option("checkpointLocation", "/home/wintersoldier/Desktop/checkpoint")
            .foreachBatch((batch: DataFrame, batchID: Long) => {
                println("The batch ID is: " + batchID)
                batch.show()
            })
            .start
            .awaitTermination()
        
        spark.close()
        
    }
    
    
}
