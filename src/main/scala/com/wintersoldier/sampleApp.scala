package com.wintersoldier


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.spark.sql.DataFrame
import javax.jms.{Connection, MessageProducer, Session, Topic}

object sampleApp {
    val spark: SparkSession = SparkSession
        .builder()
        .appName("sampleApp")
        .master("local[*]")
        .getOrCreate()
    
    import spark.implicits._
    
    spark.sparkContext.setCheckpointDir("/home/wintersoldier/Desktop/checkpoint")
    spark.sparkContext.setLogLevel("ERROR")
    
    
    // Writing to topic related part
    val clientId: String = "iAmWritingClient"
    val topicName: String = "writing2thisTopic"
    val username: String = "username"
    val password: String = "password"
    val connection: Connection = new ActiveMQConnectionFactory("username", "password", "tcp://localhost:61616").createConnection()
    val session: Session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val topic: Topic = session.createTopic("writing2thisTopic")
    val producer: MessageProducer = session.createProducer(topic)
    private var latestBatchID = -1L
    
    
    def main(array: Array[String]): Unit = {
        
        val brokerUrl: String = "tcp://localhost:61616"
        val topicName: String = "sample_topic3"
        val username: String = "username"
        val password: String = "password"
        val connectionType: String = "activemq"
        val clientId: String = "coldplay"
        val acknowledge: String = "true"
        val readInterval: String = "2000"
        
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
            //            .outputMode("append")
            //            .format("console")
            .option("checkpointLocation", "/home/wintersoldier/Desktop/checkpoint")
            .foreachBatch((batch: DataFrame, batchID: Long) => {
                println("The batch ID is: " + batchID)
                batch.show()
                writeOn(batch, batchID)
            })
            .start
            .awaitTermination()
        
        // Closing the writing part
        producer.close()
        connection.close()
        session.close()
        
        spark.close()
        
    }
    
    def writeOn(batch: DataFrame, batchId: Long): Unit = {
        if (batchId >= latestBatchID) {
            batch.foreachPartition(rowIter => {
                rowIter.foreach(
                    record => {
                        val msg = this.session.createTextMessage(record.toString())
                        producer.send(msg)
                    })
            }
            )
        }
    }
    
    
}
