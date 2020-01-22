package com.eda.spark.utils

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class DBUtil {
  def readFromPgsql(sparkSession: SparkSession, host: String, port: Int, db: String, username: String, pwd: String, sqlText: String): DataFrame = {
    val url = "jdbc:postgresql://" + host + ":" + port + "/" + db
    val prop = new Properties()
    prop.put("user", username)
    prop.put("password", pwd)
    prop.put("driver", "org.postgresql.Driver")

    var df = sparkSession.read.jdbc(url, s"(${sqlText}) t", prop)
    return df
  }

  def readFromMysql(sparkSession: SparkSession, host: String, port: Int, db: String, username: String, pwd: String, sqlText: String): DataFrame = {
    val url = "jdbc:mysql://" + host + ":" + port + "/" + db + "?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    val prop = new Properties()
    prop.put("user", username)
    prop.put("password", pwd)
    prop.put("driver", "com.mysql.jdbc.Driver")

    val df = sparkSession.read.jdbc(url, s"(${sqlText}) t", prop)
    return df
  }

  /**
   * mode: 0全量更新，1增量更新
   **/
  def writeToMysql(host: String, port: Int, db: String, username: String, pwd: String, table: String, data: DataFrame, mode: Int): Unit = {
    val url = "jdbc:mysql://" + host + ":" + port + "/" + db + "?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    val prop = new Properties()
    prop.put("user", username)
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop.put("password", pwd)

    if (mode == 0) {
      //全量更新
      clearTableFromMysql(host, port, db, username, pwd, table)//先清空数据库表中的数据，线上账号没有修改表的权限
    }

    data.write.mode(SaveMode.Append).jdbc(url, table, prop)
  }

  def clearTableFromMysql(host: String, port: Int, db: String, username: String, pwd: String, tableName: String): Unit = {
    try {
      val url = "jdbc:mysql://" + host + ":" + port + "/" + db + "?useUnicode=true&characterEncoding=utf-8&useSSL=false"
      Class.forName("com.mysql.jdbc.Driver")

      val connection: Connection = DriverManager.getConnection(url, username, pwd)
      val sql = "delete from " + tableName

      val statement = connection.prepareStatement(sql)
      statement.executeUpdate()

      if (null != statement) {
        statement.close()
      }

      if (null != connection) {
        connection.close()
      }

    } catch {
      case ex: Exception => {
        println("清表异常 Exception" + ex.printStackTrace())
      }
    }
  }
}
