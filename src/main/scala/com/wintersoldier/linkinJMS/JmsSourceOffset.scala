package com.wintersoldier.linkinJMS

import org.apache.spark.sql.execution.streaming.Offset

case class JmsSourceOffset(id:Long) extends Offset {
    
    override def json(): String = id.toString
    
}

