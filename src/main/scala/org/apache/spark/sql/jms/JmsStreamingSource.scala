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
    val clientName : String = parameters.getOrElse("clientId","client000")
    val topicName : String = parameters.getOrElse("topic", "sample_topic")
    val queueName : String = parameters.getOrElse("queue", "")
    
    val connection: Connection = DefaultSource.connectionFactory(parameters).createConnection
    connection.setClientID(clientName)
    
    val session: Session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
    var counter: LongAccumulator = sqlContext.sparkContext.longAccumulator("counter")
    
    //todo Add support for queue
    val topic : Topic = session.createTopic(topicName)
//    val subscriber : TopicSubscriber = session.createDurableSubscriber(topic, clientName)
    
    val alpha: (Any, Int) = getTheSub
    val typeOfSub: Int = getTheSub._2  // 1-> Topic 0-> Queue
    private var subscriberT : TopicSubscriber = _
    private var subscriberQ : MessageConsumer = _

    if(typeOfSub == 1)
        subscriberT = alpha._1.asInstanceOf[TopicSubscriber]
    else
        subscriberQ = alpha._2.asInstanceOf[MessageConsumer]



    def getTheSub: (Any, Int) ={
        if(topicName != "")
            {
                val topic  : Topic = session.createTopic(topicName)
                (session.createDurableSubscriber(topic, clientName), 1)
            }
        else if(topicName == "" && queueName != "")
            {
                val queue = session.createQueue(queueName)
                (session.createConsumer(queue), 0)
            }
        else
            {
                println("Neither 'queue' name nor 'topic' name passed... proceeding with 'sample_topic' ")
                val topic = session.createTopic("sample_topic")
                (session.createDurableSubscriber(topic, clientName), 1)
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
            def getTextMsg : TextMessage =  {
                if(typeOfSub == 1)
                    subscriberT.receive(RECEIVER_TIMEOUT).asInstanceOf[TextMessage]
                else
                    subscriberQ.receive(RECEIVER_TIMEOUT).asInstanceOf[TextMessage]
            }
            val textMsg : TextMessage = getTextMsg
            

            
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
