package org.apache.spark.sql.jms

import com.wintersoldier.linkinJMS._
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.util.LongAccumulator

import javax.jms._
import scala.collection.mutable.ListBuffer


class JmsStreamingSource(sqlContext: SQLContext,
                         parameters: Map[String, String],
                         metadataPath: String,
                         failOnDataLoss: Boolean
                        ) extends Source {
    
    lazy val RECEIVER_TIMEOUT: Long = parameters.getOrElse("readInterval", "1000").toLong
    val clientName: String = parameters.getOrElse("clientId", "client000")
    val topicName: String = parameters.getOrElse("topic", "")
    val queueName: String = parameters.getOrElse("queue", "")
    
    val connection: Connection = DefaultSource.connectionFactory(parameters).createConnection
    connection.setClientID(clientName)
    
    val session: Session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
    val typeOfSub: Int = getTheSub // 1-> Topic 0-> Queue
    
    if (typeOfSub == 2) {
        throw new IllegalArgumentException
    }
    private val subscriberT: Option[TopicSubscriber] = if (typeOfSub == 1) Some(session.createDurableSubscriber(session.createTopic(topicName), clientName)) else None
    private val subscriberQ: Option[MessageConsumer] = if (typeOfSub == 0) Some(session.createConsumer(session.createQueue(queueName))) else None
    var counter: LongAccumulator = sqlContext.sparkContext.longAccumulator("counter")
    
    def getTheSub: Int = {
        if (topicName.trim != "") {
            1
        }
        else if (topicName.trim == "" && queueName.trim != "") {
            0
        }
        else {
            println("<><><><><><>ERROR: Neither 'queue' name nor 'topic' name passed<><><><><><>")
            2
        }
    }
    
    
    connection.start()
    
    override def getOffset: Option[Offset] = {
        counter.add(1)
        Some(JmsSourceOffset(counter.value))
    }
    
    override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        
        var break = true
        val messageList: ListBuffer[JmsMessage] = ListBuffer()
        while (break) {
            
            def getTextMsg: TextMessage = {
                if (typeOfSub == 1)
                    subscriberT.get.receive(RECEIVER_TIMEOUT).asInstanceOf[TextMessage]
                else
                    subscriberQ.get.receive(RECEIVER_TIMEOUT).asInstanceOf[TextMessage]
            }
            
            val textMsg: TextMessage = getTextMsg
            
            
            
            // the below code is to test the acknowledgement of individual messages
            /*            if(textMsg!=null && textMsg.getText == "testingFail")
                            {
                                val iota : Int = 3/0
                            }*/
            
            // I am using this line to acknowledge individual textMessages
            if (parameters.getOrElse("acknowledge", "false").toBoolean && textMsg != null) {
                textMsg.acknowledge()
            }
            textMsg match {
                case null => break = false
                case _ => messageList += JmsMessage(textMsg)
            }
        }
        import org.apache.spark.unsafe.types.UTF8String._
        val internalRDD = messageList.map(message => InternalRow(
            fromString(message.content),
            fromString(message.correlationId),
            fromString(message.jmsType),
            fromString(message.messageId),
            fromString(message.queue)
        ))
        val rdd = sqlContext.sparkContext.parallelize(internalRDD)
        sqlContext.internalCreateDataFrame(rdd, schema = schema, isStreaming = true)
        
    }
    
    override def schema: StructType = {
        ScalaReflection.schemaFor[JmsMessage].dataType.asInstanceOf[StructType]
    }
    
    override def stop(): Unit = {
        session.close()
        connection.close()
    }
    
}
