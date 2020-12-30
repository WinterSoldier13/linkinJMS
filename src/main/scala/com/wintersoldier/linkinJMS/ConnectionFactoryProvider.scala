package com.wintersoldier.linkinJMS

import org.apache.activemq.ActiveMQConnectionFactory

import javax.jms.ConnectionFactory


trait ConnectionFactoryProvider {
    def createConnection(options:Map[String,String]):ConnectionFactory
}

class AMQConnectionFactoryProvider extends ConnectionFactoryProvider {
    
    override def createConnection(options: Map[String, String]): ConnectionFactory = {
//        new ActiveMQConnectionFactory()
        val activeOb = new ActiveMQConnectionFactory(options("username"), options("password"), options("brokerUrl"))
        activeOb
    }
}

