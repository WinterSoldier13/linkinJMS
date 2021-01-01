package com.wintersoldier.linkinJMS

import org.apache.activemq.ActiveMQConnectionFactory

import javax.jms.ConnectionFactory


trait ConnectionFactoryProvider {
    def createConnection(options:Map[String,String]):ConnectionFactory
}

class AMQConnectionFactoryProvider extends ConnectionFactoryProvider {
    
    override def createConnection(options: Map[String, String]): ConnectionFactory = {
        val brokerUrl : String = options.getOrElse("brokerUrl", "")
        val username : String = options.getOrElse("username", "")
        val password : String = options.getOrElse("password", "")
        
        if(brokerUrl == "")
            {
                println("'brokerUrl' parameter not passed, proceeding with default value")
                new ActiveMQConnectionFactory()
            }
        if(username == "" || password == "")
            {
                println("'username' or 'password' not passed")
                 val object_ = new ActiveMQConnectionFactory(brokerUrl)
                 return object_
            }
            
        val activeOb = new ActiveMQConnectionFactory(username, password, brokerUrl)
        activeOb
    }
}

