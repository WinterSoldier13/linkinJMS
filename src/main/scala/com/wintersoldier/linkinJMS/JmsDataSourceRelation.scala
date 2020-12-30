package com.wintersoldier.linkinJMS

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

import javax.jms._
import scala.collection.mutable.ListBuffer


class JmsDataSourceRelation(override val sqlContext: SQLContext, parameters: Map[String, String]) extends BaseRelation with TableScan with Serializable
{
    lazy val RECEIVER_TIMEOUT: Long = parameters.getOrElse("receiver.timeout","3000").toLong
    
    
    override def schema: StructType = {
        ScalaReflection.schemaFor[JmsMessage].dataType.asInstanceOf[StructType]
    }
    
    override def buildScan(): RDD[Row] = {
        val connection = DefaultSource.connectionFactory(parameters).createConnection
        val session: Session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
        connection.start()
        var messageRdd:RDD[Row] = null
        try {
            val queue = session.createQueue(parameters("queue"))
            val consumer = session.createConsumer(queue)
            var break = true
            val messageList :ListBuffer[JmsMessage] = ListBuffer()
            while (break) {
                val textMsg = consumer.receive(RECEIVER_TIMEOUT).asInstanceOf[TextMessage]
                /*textMsg  match {
                  case msg:TextMessage =>
                  case msg:BytesMessage => {
                    var byteData:Array[Byte] = null
                    byteData = new Array[Byte](msg.getBodyLength.asInstanceOf[Int])
                    msg.readBytes(byteData)
                  }
                  case msg:ObjectMessage=> msg.getObject
                  case msg:StreamMessage =>
                  case msg:MapMessage =>
                }*/
                if(parameters.getOrElse("acknowledge","false").toBoolean && textMsg !=null){
                    textMsg.acknowledge()
                }
                textMsg match {
                    case null => break = false
                    case _ =>  messageList += JmsMessage(textMsg)
                }
            }
            import scala.collection.JavaConverters._
            val messageDf = sqlContext.createDataFrame(messageList.toList.asJava,classOf[JmsMessage])
            messageRdd = messageDf.rdd
        } finally {
            connection.close()
        }
        messageRdd
    }
}

