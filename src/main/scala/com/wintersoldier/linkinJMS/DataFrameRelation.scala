package com.wintersoldier.linkinJMS

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class DataFrameRelation(override val sqlContext: SQLContext, data:DataFrame) extends BaseRelation with TableScan with Serializable {
    
    override def schema: StructType = {
        data.schema
    }
    
    override def buildScan(): RDD[Row] = {
        data.rdd
    }
    
}
