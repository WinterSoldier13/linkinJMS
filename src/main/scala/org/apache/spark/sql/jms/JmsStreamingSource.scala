package org.apache.spark.sql.jms

import com.wintersoldier.linkinJMS._
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.util.LongAccumulator
import java.util.Random
import javax.jms._
import scala.collection.mutable.ListBuffer


class JmsStreamingSource(sqlContext: SQLContext,
                         parameters: Map[String, String],
                         metadataPath: String,
                         failOnDataLoss: Boolean
                        ) extends Source {
    
    lazy val RECEIVER_INTERVAL: Long = parameters.getOrElse("readInterval", "1000").toLong
    val clientName: String = parameters.getOrElse("clientId", "")
    val srcType: String = parameters.getOrElse("mqSrcType", "")
    val srcName: String = parameters.getOrElse("mqSrcName", "")
    
    val connection: Connection = DefaultSource.connectionFactory(parameters).createConnection()
    if (clientName != "")
        connection.setClientID(clientName)
    else
        println("[WARN] No 'clientId' passed, this will result a nonDurable connection in case of Topic ")
    
    // if transacted is set to true then itt does not matter which ACK you pass to the function
    // you should just use session.commit()
    // to recover call session.recover()/ session.rollback()
    val session: Session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE)
    
    // storing the session into a variable for later use
//    JmsSessionManager.setSession(sess = session)
    
    val typeOfSub: Int = getTheSub // 1-> Topic 0-> Queue 2-> Not specified
    
    if (typeOfSub == 2) {
        println("<><><><><><> [ERROR] type 'queue'/'topic' not passed <><><><><><>")
        throw new IllegalArgumentException
    }
    
    private val subscriberT: Option[TopicSubscriber] = if (typeOfSub == 1) Some(getSubscriberT) else None
    private val subscriberQ: Option[MessageConsumer] = if (typeOfSub == 0) Some(getConsumerQ) else None
    
    
    private def getConsumerQ: MessageConsumer = {
        session.createConsumer(session.createQueue(srcName))
    }
    
    var counter: LongAccumulator = sqlContext.sparkContext.longAccumulator("counter")
    
    def getSubscriberT: TopicSubscriber = {
        if (clientName == "") {
            val random: Random = new Random()
            val rand1 = random.nextInt(10000)
            val rand2 = random.nextInt(100000)
            session.createDurableSubscriber(session.createTopic(srcName), s"default_client$rand1$rand2")
        } else
            session.createDurableSubscriber(session.createTopic(srcName), clientName)
    }
    
    def getTheSub: Int = {
        if (srcType == "topic") {
            1
        }
        else if (srcType == "queue") {
            0
        }
        else {
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
                    subscriberT.get.receive(RECEIVER_INTERVAL).asInstanceOf[TextMessage]
                else
                    subscriberQ.get.receive(RECEIVER_INTERVAL).asInstanceOf[TextMessage]
            }
            
            val textMsg: TextMessage = getTextMsg
            
            
            // the below code is to test the acknowledgement of individual messages
            //                        if(textMsg!=null && textMsg.getText == "testingFail")
            //                            {
            //                                val iota : Int = 3/0
            //                            }
            
            // I am using this line to acknowledge individual textMessages
            // shift this
            if (parameters.getOrElse("acknowledge", "false").toBoolean && textMsg != null) {
                //                textMsg.acknowledge() // use this with client_ack
                session.commit() // use this while doing transacted session
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
            fromString(message.queue))
        )
        
        val rdd = sqlContext.sparkContext.parallelize(internalRDD)
        sqlContext.internalCreateDataFrame(rdd, schema = schema, isStreaming = true)
        
    }
    
    override def schema: StructType = {
        ScalaReflection.schemaFor[JmsMessage].dataType.asInstanceOf[StructType]
    }
    
    override def stop(): Unit = {
        session.close()
        JmsSessionManager.setSession(null)
        connection.close()
    }
    
}
