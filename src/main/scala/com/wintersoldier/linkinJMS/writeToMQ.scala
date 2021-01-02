package com.wintersoldier.linkinJMS

import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.jms.{Connection, MessageProducer, Session, Topic}

class writeToMQ(implicit spark : SparkSession) extends Serializable
{
    import spark.implicits._
    private var clientId: String = "iAmWritingClient"
    private var topicName: String = "writing2thisTopic"
    private var username: String = "username"
    private var password: String = "password"
    private var brokerURL : String = "tcp://localhost:61616"
    private var connection: Connection = _
    private var session: Session = _
    private var topic: Topic = _
    private var producer: MessageProducer = _
    private var latestBatchID = -1L
    
    def __init__(brokerURL:String, clientId: String, topicName: String, username: String, password: String): Unit = {
        
        this.brokerURL = brokerURL
        this.clientId = clientId
        this.topicName = topicName
        this.username = username
        this.password = password
        createConnections()
    }
    
    def createConnections(): Unit = {
        this.connection = new ActiveMQConnectionFactory(this.username, this.password, this.brokerURL).createConnection()
        this.connection.setClientID(this.clientId)
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
        this.topic = session.createTopic(this.topicName)
        this.producer = session.createProducer(topic)
        println("connection successful")
    }
    
    
    def writeOn(batch: DataFrame, batchId: Long): Unit = {
        if (batchId >= this.latestBatchID) {
            batch.foreachPartition(rowIter => {
                rowIter.foreach(
                    record => {
                        val msg = this.session.createTextMessage(record.toString())
                        producer.send(msg)
                    })
            }
            )
            this.latestBatchID = batchId
        }
    }
    
    def directWrite(df: DataFrame): Unit =
    {
        createConnections()
        df.writeStream
            .option("checkpointLocation", "/home/wintersoldier/Desktop/checkpoint")
            .foreachBatch((batch: DataFrame, batchID: Long) => {
                println("The batch ID is: " + batchID)
                batch.show()
                writeOn(batch, batchID)
            })
            .start
            .awaitTermination()
    }
    
    def closeConnection(): Unit =
    {
        this.producer.close()
        this.connection.close()
        this.session.close()
    }
    
}
