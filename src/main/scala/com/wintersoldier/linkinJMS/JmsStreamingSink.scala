package com.wintersoldier.linkinJMS

import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext}
import javax.jms.{Connection, MessageProducer, Session, Topic}



/*
* It's a MESS... Don't look here instead goto writeToMQ.scala
* */






class JmsStreamingSink(sqlContext: SQLContext,
                       parameters: Map[String, String]
                      ) extends Sink {
    
    @volatile private var latestBatchId = -1L
    
    val clientID : String = parameters.getOrElse("clientId", "client000")
    val topicName : String = parameters.getOrElse("topic", "")
    val queueName :String = parameters.getOrElse("queue", "")
    
    val connection: Connection = DefaultSource.connectionFactory(parameters).createConnection
    connection.setClientID(clientID)
    val session: Session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    
    val producer: MessageProducer = getProducer(session= session, topicName = topicName, queueName = queueName)
    
    override def addBatch(batchId: Long, data: DataFrame): Unit = {
        if (batchId <= latestBatchId)
        {
        
        }
        else {
            data.foreachPartition(rowIter => {
                rowIter.foreach(
                    record => {
                        val msg = session.createTextMessage(record.toString())
                        producer.send(msg)
                    })
                producer.close()
                connection.close()
                session.close()
            }
            )
            latestBatchId = batchId
        }
    }
    
    def getProducer(session: Session, topicName: String, queueName : String): MessageProducer =
    {
        if(topicName != "")
            {
                val topic: Topic = session.createTopic(topicName)
                session.createProducer(topic)
            }
        else if(topicName == "" && queueName != "")
            {
                val queue = session.createQueue(queueName)
                session.createProducer(queue)
            }
            else
            {
                val sampleTopic = session.createTopic("sample_topic")
                println("'topic' name and 'queue' name is not defined... proceeding with default topic name 'sample_topic'")
                session.createProducer(sampleTopic)
            }
    }
    
    
}