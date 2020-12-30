package com.wintersoldier.linkinJMS

import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext}

import javax.jms.Session


class JmsStreamingSink(sqlContext: SQLContext,
                       parameters: Map[String, String]
                      ) extends Sink {
    
    @volatile private var latestBatchId = -1L
    
    
    override def addBatch(batchId: Long, data: DataFrame): Unit = {
        if (batchId <= latestBatchId) {
        
        } else {
            data.foreachPartition(rowIter => {
                val connection = DefaultSource.connectionFactory(parameters).createConnection
                val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
                val queue = session.createQueue(parameters.getOrElse("queue",throw new IllegalArgumentException("Option queue is required")))
                val producer = session.createProducer(queue)
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
    
}