package com.eda.spark.sql.smartvillage

import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.DataTypes
import java.util.{Calendar, Date, Properties}

object HiveExecutor extends Serializable {
  def main(args: Array[String]): Unit = {
//    val sqlContext = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).enableHiveSupport().getOrCreate()
    val sqlContext = SparkSession.builder().appName(this.getClass.getSimpleName).enableHiveSupport().getOrCreate()
    sqlContext.sparkContext.setLogLevel("WARN")

    val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
    var syncMode = 0

    if (today > "2020-01-25") { //上线发布日期
      syncMode = 1
    }

    executeICCard(sqlContext, syncMode)
    executeCarParking(sqlContext, syncMode)
    executeDeviceAuth(sqlContext, syncMode)
    executeDeviceAccess(sqlContext, syncMode)
    executePayment(sqlContext, syncMode)
    executeBaseCourt(sqlContext, syncMode)
  }

  def executeICCard(sparkSession: SparkSession, mode: Int): Unit = {
    var sql: String = ""
    if (mode == 0) {
      //全量更新
      sql = "SELECT rowkey, cardtype, owneruuid, ownertypetext, courtid, cardstatustext, updatetime " +
        "FROM community.community_idcarddetail " +
        "WHERE cardtype !='' " +
        "AND owneruuid !='' " +
        "AND ownertypetext !='' " +
        "AND courtid !='' " +
        "AND cardstatustext !=''"
    } else {
      //增量更新，只拉取昨日数据
      sql = "SELECT rowkey, cardtype, owneruuid, ownertypetext, courtid, cardstatustext, updatetime " +
        "FROM community.community_idcarddetail " +
        "WHERE cardtype!='' " +
        "AND owneruuid!='' " +
        "AND ownertypetext !='' " +
        "AND courtid !='' " +
        "AND cardstatustext !='' " +
        "AND updatetime BETWEEN from_unixtime(unix_timestamp()-1*60*60*24, 'yyyy-MM-dd') AND from_unixtime(unix_timestamp(), 'yyyy-MM-dd')"
    }

    val df = sparkSession.sql(sql)
    df.cache().createOrReplaceTempView("ic_card")

    //IC持卡人分布
    var resultDf = sparkSession.sql("SELECT cardtype, ownertypetext, courtid, COUNT(DISTINCT rowkey) as countNum FROM ic_card GROUP BY cardtype, ownertypetext, courtid")
    writeToMysql("cbox_smartvillage_v0.1", "ic_card_user_result", resultDf, mode)

    //IC状态分布
    resultDf = sparkSession.sql("SELECT cardtype, cardstatustext, courtid, COUNT(DISTINCT rowkey) as countNum FROM ic_card GROUP BY cardtype, cardstatustext, courtid")
    writeToMysql("cbox_smartvillage_v0.1", "ic_card_status_result", resultDf, mode)
  }

  //  def executeICCard(sparkSession: SparkSession): Unit = {
  //    val sql = "SELECT rowkey, cardtype, owneruuid, ownertypetext, cardstatustext FROM community.community_idcarddetail " +
  //      "WHERE cardtype!='' AND owneruuid!='' AND ownertypetext !='' AND cardstatustext !=''"
  //    val df = sparkSession.sql(sql)
  //    df.cache().createOrReplaceTempView("ic_card")
  //    df.show()
  //
  //    //IC持卡人分布
  //    var resultDf = sparkSession.sql("SELECT cardtype, ownertypetext, COUNT(DISTINCT rowkey) as countNum FROM ic_card GROUP BY cardtype, ownertypetext")
  //    writeToMysql("cbox_smartvillage_v0.1", "ic_card_user_result", resultDf)
  //
  //    //IC状态分布
  //    resultDf = sparkSession.sql("SELECT cardtype, cardstatustext,COUNT(DISTINCT rowkey) as countNum FROM ic_card GROUP BY cardtype, cardstatustext")
  //    writeToMysql("cbox_smartvillage_v0.1", "ic_card_status_result", resultDf)
  //  }

  def executeCarParking(sparkSession: SparkSession, mode: Int): Unit = {
    var sql: String = ""
    if (mode == 0) {
      //全量更新
      sql = "SELECT rowkey, carnum, stopTime, intime, outtime, courtid, entermodetext, exitmodetext, carporttypetext, consumemoney, payedmoney " +
        "FROM community.community_car " +
        "WHERE carporttypetext!='' " +
        "AND exitmodetext!='' " +
        "AND entermodetext!='' " +
        "AND stoptime!='' " +
        "AND intime!='' " +
        "AND outtime!='' " +
        "AND courtid!='' " +
        "AND carnum!='' " +
        "AND payedmoney!=''"
    } else {
      //增量更新，只拉取昨日数据
      sql = "SELECT rowkey, carnum, stopTime, intime, outtime, courtid, entermodetext, exitmodetext, carporttypetext, consumemoney, payedmoney " +
        "FROM community.community_car " +
        "WHERE carporttypetext!='' " +
        "AND exitmodetext!='' " +
        "AND entermodetext!='' " +
        "AND stoptime!='' " +
        "AND intime!='' " +
        "AND outtime!='' " +
        "AND courtid!='' " +
        "AND payedmoney!='' " +
        "AND carnum!='' " +
        "AND outtime BETWEEN from_unixtime(unix_timestamp()-1*60*60*24, 'yyyy-MM-dd') AND from_unixtime(unix_timestamp(), 'yyyy-MM-dd')"
    }

    val df = sparkSession.sql(sql)
    df.cache().createOrReplaceTempView("car_parking")

    sparkSession.udf.register("stopTimeLevel", (str: String) => {
      try {
        if (str.contains("天")) {
          "24小时以上"
        } else if (str.contains("小时")) {
          val hour: Int = str.split("小时")(0).toInt
          if (hour == 1) {
            "1-2小时"
          } else if (hour >= 2 && hour < 4) {
            "2-4小时"
          } else if (hour >= 4 && hour < 12) {
            "4-12小时"
          } else if (hour >= 12 && hour < 24) {
            "12-24小时"
          } else {
            "其他"
          }
        } else {
          "1小时内"
        }
      } catch {
        case e: Exception => "其他"
      }
    })

    sparkSession.udf.register("intimeLevel", (str: String) => {
      try {
        val hour: Int = str.split(" ")(1).split(":")(0).toInt
        if (hour >= 0 && hour < 8) {
          "0-8点"
        } else if (hour >= 8 && hour < 10) {
          "8-10点"
        } else if (hour >= 10 && hour < 12) {
          "10-12点"
        } else if (hour >= 12 && hour < 14) {
          "12-14点"
        } else if (hour >= 14 && hour < 18) {
          "14-18点"
        } else if (hour >= 18 && hour < 22) {
          "18-22点"
        } else if (hour >= 22 && hour < 24) {
          "22-34点"
        } else {
          "其他"
        }
      } catch {
        case e: Exception => "其他"
      }
    })

    sparkSession.udf.register("timeDay", (str: String) => {
      try {
        str.split(" ")(0)
      } catch {
        case e: Exception => "其他"
      }
    })

    sparkSession.udf.register("carEnergy", (str: String) => {
      try {
        if(str.length > 7) {
          "新能源"
        } else {
          "非新能源"
        }
      } catch {
        case e: Exception => "其他"
      }
    })

    val tempResult = sparkSession.sql("SELECT rowkey, carnum, carEnergy(carnum) car_energy, stopTimeLevel(stopTime) stopTimeLevel, intimeLevel(intime) intimeLevel, timeDay(outtime) outtime, entermodetext, exitmodetext, carporttypetext, consumemoney, payedmoney, courtid FROM car_parking")
    tempResult.createOrReplaceTempView("car_parking")
    tempResult.show()

    //机动车停车时长分布
    var resultDf = sparkSession.sql("SELECT stopTimeLevel, courtid, COUNT(DISTINCT rowkey) as countNum FROM car_parking GROUP BY stopTimeLevel, courtid")
    writeToMysql("cbox_smartvillage_v0.1", "car_parking_stoptime_result", resultDf, mode)

    //机动车入场时间分布
    resultDf = sparkSession.sql("SELECT intimeLevel, courtid, COUNT(DISTINCT rowkey) as countNum FROM car_parking GROUP BY intimeLevel, courtid")
    writeToMysql("cbox_smartvillage_v0.1", "car_parking_intime_result", resultDf, mode)

    //机动车出场开闸方式
    resultDf = sparkSession.sql("SELECT exitmodetext, courtid, COUNT(DISTINCT rowkey) as countNum FROM car_parking GROUP BY exitmodetext, courtid")
    writeToMysql("cbox_smartvillage_v0.1", "car_parking_exitmode_result", resultDf, mode)

    //车辆类型比例
    resultDf = sparkSession.sql("SELECT carporttypetext, courtid, COUNT(DISTINCT rowkey) as countNum FROM car_parking GROUP BY carporttypetext, courtid")
    writeToMysql("cbox_smartvillage_v0.1", "car_parking_parkingtype_result", resultDf, mode)

    //车辆缴费
    resultDf = sparkSession.sql("SELECT outtime, courtid, SUM(payedmoney) as payedmoney_total, SUM(consumemoney) as consumemoney_total FROM car_parking GROUP BY outtime, courtid")
    writeToMysql("cbox_smartvillage_v0.1", "car_parking_pay_result", resultDf, mode)

    //新能源车通行
    resultDf = sparkSession.sql("SELECT outtime, courtid, carnum, car_energy, COUNT(DISTINCT rowkey) as countNum FROM car_parking GROUP BY car_energy, outtime, courtid, carnum")
    writeToMysql("cbox_smartvillage_v0.1", "car_parking_energy_result", resultDf, mode)
  }

  //  def executeCarParking(sparkSession: SparkSession): Unit = {
  //    var sql = "SELECT rowkey, stopTime, intime, outtime, entermodetext, exitmodetext, carporttypetext, consumemoney, payedmoney " +
  //      "FROM community.community_car " +
  //      "WHERE carporttypetext!='' AND exitmodetext!='' AND entermodetext!='' AND stoptime!='' AND intime!='' AND outtime!='' AND payedmoney!=''"
  //    val df = sparkSession.sql(sql)
  //    df.cache().createOrReplaceTempView("car_parking")
  //    //    df.show()
  //
  //    sparkSession.udf.register("stopTimeLevel", (str: String) => {
  //      try {
  //        if (str.contains("天")) {
  //          "24小时以上"
  //        } else if (str.contains("小时")) {
  //          val hour: Int = str.split("小时")(0).toInt
  //          if (hour == 1) {
  //            "1-2小时"
  //          } else if (hour >= 2 && hour < 4) {
  //            "2-4小时"
  //          } else if (hour >= 4 && hour < 12) {
  //            "4-12小时"
  //          } else if (hour >= 12 && hour < 24) {
  //            "12-24小时"
  //          } else {
  //            "其他"
  //          }
  //        } else {
  //          "1小时内"
  //        }
  //      } catch {
  //        case e: Exception => "其他"
  //      }
  //    })
  //
  //    sparkSession.udf.register("intimeLevel", (str: String) => {
  //      try {
  //        val hour: Int = str.split(" ")(1).split(":")(0).toInt
  //        if (hour >= 0 && hour < 8) {
  //          "0-8点"
  //        } else if (hour >= 8 && hour < 10) {
  //          "8-10点"
  //        } else if (hour >= 10 && hour < 12) {
  //          "10-12点"
  //        } else if (hour >= 12 && hour < 14) {
  //          "12-14点"
  //        } else if (hour >= 14 && hour < 18) {
  //          "14-18点"
  //        } else if (hour >= 18 && hour < 22) {
  //          "18-22点"
  //        } else if (hour >= 22 && hour < 24) {
  //          "22-34点"
  //        } else {
  //          "其他"
  //        }
  //      } catch {
  //        case e: Exception => "其他"
  //      }
  //    })
  //
  //    sparkSession.udf.register("timeDay", (str: String) => {
  //      try {
  //        str.split(" ")(0)
  //      } catch {
  //        case e: Exception => "其他"
  //      }
  //    })
  //
  //    val tempResult = sparkSession.sql("SELECT rowkey, stopTimeLevel(stopTime) stopTimeLevel, intimeLevel(intime) intimeLevel, timeDay(outtime) outtime, entermodetext, exitmodetext, carporttypetext, consumemoney, payedmoney FROM car_parking")
  //    tempResult.createOrReplaceTempView("car_parking")
  //    tempResult.show()
  //
  //    //机动车停车时长分布
  //    var resultDf = sparkSession.sql("SELECT stopTimeLevel, COUNT(DISTINCT rowkey) as countNum FROM car_parking GROUP BY stopTimeLevel")
  //    writeToMysql("cbox_smartvillage_v0.1", "car_parking_stoptime_result", resultDf)
  //
  //    //机动车入场时间分布
  //    resultDf = sparkSession.sql("SELECT intimeLevel, COUNT(DISTINCT rowkey) as countNum FROM car_parking GROUP BY intimeLevel")
  //    writeToMysql("cbox_smartvillage_v0.1", "car_parking_intime_result", resultDf)
  //
  //    //机动车出场开闸方式
  //    resultDf = sparkSession.sql("SELECT exitmodetext, COUNT(DISTINCT rowkey) as countNum FROM car_parking GROUP BY exitmodetext")
  //    writeToMysql("cbox_smartvillage_v0.1", "car_parking_exitmode_result", resultDf)
  //
  //    //车辆类型比例
  //    resultDf = sparkSession.sql("SELECT carporttypetext, COUNT(DISTINCT rowkey) as countNum FROM car_parking GROUP BY carporttypetext")
  //    writeToMysql("cbox_smartvillage_v0.1", "car_parking_parkingtype_result", resultDf)
  //
  //    //车辆缴费
  //    resultDf = sparkSession.sql("SELECT outtime, SUM(payedmoney) as payedmoney_total, SUM(consumemoney) as consumemoney_total FROM car_parking GROUP BY outtime")
  //    writeToMysql("cbox_smartvillage_v0.1", "car_parking_pay_result", resultDf)
  //  }

  def executeDeviceAuth(sparkSession: SparkSession, mode: Int): Unit = {
    var sql: String = ""

    if (mode == 0) {
      //全量更新
      sql = "SELECT rowkey, usertype, devicename, subdevicename, credencetype, courtuuid, updatetime " +
        "FROM community.community_deviceauthdetail " +
        "WHERE usertype!='' " +
        "AND devicename!='' " +
        "AND subdevicename!='' " +
        "AND courtuuid!='' " +
        "AND credencetype!=''"
    } else {
      //增量更新，只拉取昨日数据
      sql = "SELECT rowkey, usertype, devicename, subdevicename, credencetype, courtuuid, updatetime " +
        "FROM community.community_deviceauthdetail " +
        "WHERE usertype!='' " +
        "AND devicename!='' " +
        "AND subdevicename!='' " +
        "AND courtuuid!='' " +
        "AND credencetype!='' " +
        "AND updatetime BETWEEN from_unixtime(unix_timestamp()-1*60*60*24, 'yyyy-MM-dd') AND from_unixtime(unix_timestamp(), 'yyyy-MM-dd')"
    }

    val df = sparkSession.sql(sql)
    df.cache().createOrReplaceTempView("device_auth")

    sparkSession.udf.register("userTypeConverter", (value: Int) => {
      try {
        if (value == 1) {
          "业主"
        } else if (value == 2) {
          "家属"
        } else if (value == 3) {
          "租客"
        } else if (value == 4) {
          "访客"
        } else if (value == 5) {
          "物业"
        } else if (value == 6) {
          "快递"
        } else if (value == 7) {
          "外卖"
        } else {
          "其他"
        }
      } catch {
        case e: Exception => "其他"
      }
    })

    sparkSession.udf.register("credenceTypeConverter", (value: Int) => {
      try {
        if (value == 5) {
          "车牌"
        } else if (value == 2 || value == 3) {
          "卡片"
        } else if (value == 7) {
          "人脸"
        } else if (value == 6) {
          "指纹"
        } else if (value == 8) {
          "二维码"
        } else if (value == 9) {
          "蓝牙"
        } else if (value == 12) {
          "NFC"
        } else if (value == 10) {
          "密码"
        } else {
          "其他"
        }
      } catch {
        case e: Exception => "其他"
      }
    })

    val tempResult = sparkSession.sql("SELECT rowkey, courtuuid, userTypeConverter(usertype) usertype, devicename, subdevicename, credenceTypeConverter(credencetype) credencetype FROM device_auth")
    tempResult.createOrReplaceTempView("device_auth")
    tempResult.show()

    //人员类型比例
    var resultDf = sparkSession.sql("SELECT usertype, courtuuid, credencetype, COUNT(DISTINCT rowkey) as countNum FROM device_auth GROUP BY usertype, credencetype, courtuuid")
    writeToMysql("cbox_smartvillage_v0.1", "device_auth_usertype_result", resultDf, mode)

    //设备类型比例
    resultDf = sparkSession.sql("SELECT devicename, courtuuid, COUNT(DISTINCT rowkey) as countNum FROM device_auth GROUP BY devicename, courtuuid")
    writeToMysql("cbox_smartvillage_v0.1", "device_auth_devicetype_result", resultDf, mode)

    //凭证类型比例
    resultDf = sparkSession.sql("SELECT credencetype, courtuuid, COUNT(DISTINCT rowkey) as countNum FROM device_auth GROUP BY credencetype, courtuuid")
    writeToMysql("cbox_smartvillage_v0.1", "device_auth_credencetype_result", resultDf, mode)
  }

  //  def executeDeviceAuth(sparkSession: SparkSession): Unit = {
  //    val sql = "SELECT rowkey, usertype, devicename, subdevicename, credencetype FROM community.community_deviceauthdetail WHERE usertype!='' AND devicename!='' AND subdevicename!='' AND credencetype!=''"
  //    val df = sparkSession.sql(sql)
  //    df.cache().createOrReplaceTempView("device_auth")
  //    //    df.show()
  //
  //    sparkSession.udf.register("userTypeConverter", (value: Int) => {
  //      try {
  //        if (value == 1) {
  //          "业主"
  //        } else if (value == 2) {
  //          "家属"
  //        } else if (value == 3) {
  //          "租客"
  //        } else if (value == 4) {
  //          "访客"
  //        } else if (value == 5) {
  //          "物业"
  //        } else if (value == 6) {
  //          "快递"
  //        } else if (value == 7) {
  //          "外卖"
  //        } else {
  //          "其他"
  //        }
  //      } catch {
  //        case e: Exception => "其他"
  //      }
  //    })
  //
  //    sparkSession.udf.register("credenceTypeConverter", (value: Int) => {
  //      try {
  //        if (value == 5) {
  //          "车牌"
  //        } else if (value == 2 || value == 3) {
  //          "卡片"
  //        } else if (value == 7) {
  //          "人脸"
  //        } else if (value == 6) {
  //          "指纹"
  //        } else if (value == 8) {
  //          "二维码"
  //        } else if (value == 9) {
  //          "蓝牙"
  //        } else if (value == 12) {
  //          "NFC"
  //        } else if (value == 10) {
  //          "密码"
  //        } else {
  //          "其他"
  //        }
  //      } catch {
  //        case e: Exception => "其他"
  //      }
  //    })
  //
  //    val tempResult = sparkSession.sql("SELECT rowkey, userTypeConverter(usertype) usertype, devicename, subdevicename, credenceTypeConverter(credencetype) credencetype FROM device_auth")
  //    tempResult.createOrReplaceTempView("device_auth")
  //    tempResult.show()
  //
  //    //人员类型比例
  //    var resultDf = sparkSession.sql("SELECT usertype, credencetype, COUNT(DISTINCT rowkey) as countNum FROM device_auth GROUP BY usertype, credencetype")
  //    writeToMysql("cbox_smartvillage_v0.1", "device_auth_usertype_result", resultDf)
  //
  //    //设备类型比例
  //    resultDf = sparkSession.sql("SELECT devicename, COUNT(DISTINCT rowkey) as countNum FROM device_auth GROUP BY devicename")
  //    writeToMysql("cbox_smartvillage_v0.1", "device_auth_devicetype_result", resultDf)
  //
  //    //凭证类型比例
  //    resultDf = sparkSession.sql("SELECT credencetype, COUNT(DISTINCT rowkey) as countNum FROM device_auth GROUP BY credencetype")
  //    writeToMysql("cbox_smartvillage_v0.1", "device_auth_credencetype_result", resultDf)
  //  }

  def executeDeviceAccess(sparkSession: SparkSession, mode: Int): Unit = {
    var sql: String = ""

    if (mode == 0) {
      //全量更新
      sql = "SELECT rowkey, courtuuid, userid, usertype, devicename, updatetime, description " +
        "from community.community_deviceacessdetail " +
        "WHERE userid!='' " +
        "AND usertype!='' " +
        "AND devicename!='' " +
        "AND updatetime!='' " +
        "AND courtuuid!='' " +
        "AND description!=''"
    } else {
      //增量更新，只拉取昨日数据
      sql = "SELECT rowkey, courtuuid, userid, usertype, devicename, updatetime, description " +
        "from community.community_deviceacessdetail " +
        "WHERE userid!='' " +
        "AND usertype!='' " +
        "AND devicename!='' " +
        "AND updatetime!='' " +
        "AND courtuuid!='' " +
        "AND description!='' " +
        "AND updatetime BETWEEN from_unixtime(unix_timestamp()-1*60*60*24, 'yyyy-MM-dd') AND from_unixtime(unix_timestamp(), 'yyyy-MM-dd')"
    }

    val df = sparkSession.sql(sql)
    df.cache().createOrReplaceTempView("device_access")

    sparkSession.udf.register("userTypeConverter", (value: Int) => {
      try {
        if (value == 1) {
          "业主"
        } else if (value == 2) {
          "家属"
        } else if (value == 3) {
          "租客"
        } else if (value == 4) {
          "访客"
        } else if (value == 5) {
          "物业"
        } else if (value == 6) {
          "快递"
        } else if (value == 7) {
          "外卖"
        } else {
          "其他"
        }
      } catch {
        case e: Exception => "其他"
      }
    })

    sparkSession.udf.register("updatetimeLevel", (str: String) => {
      try {
        val hour: Int = str.split(" ")(1).split(":")(0).toInt
        if (hour >= 0 && hour < 8) {
          "0-8点"
        } else if (hour >= 8 && hour < 10) {
          "8-10点"
        } else if (hour >= 10 && hour < 12) {
          "10-12点"
        } else if (hour >= 12 && hour < 14) {
          "12-14点"
        } else if (hour >= 14 && hour < 18) {
          "14-18点"
        } else if (hour >= 18 && hour < 22) {
          "18-22点"
        } else if (hour >= 22 && hour < 24) {
          "22-34点"
        } else {
          "其他"
        }
      } catch {
        case e: Exception => "其他"
      }
    })

    sparkSession.udf.register("timeDay", (str: String) => {
      try {
        str.split(" ")(0)
      } catch {
        case e: Exception => "其他"
      }
    })

    val tempResult = sparkSession.sql("SELECT rowkey, courtuuid, userid, userTypeConverter(usertype) usertype, devicename, updatetimeLevel(updatetime) updatetimeLevel, timeDay(updatetime) updatetimeDay, description from device_access")
    tempResult.createOrReplaceTempView("device_access")
    tempResult.show()


    //人员类型趋势
    var resultDf = sparkSession.sql("SELECT usertype, updatetimeDay, courtuuid, COUNT(DISTINCT rowkey) as countNum FROM device_access GROUP BY usertype, updatetimeDay, courtuuid")
    writeToMysql("cbox_smartvillage_v0.1", "device_access_usertype_result", resultDf, mode)

    //设备类型趋势
    resultDf = sparkSession.sql("SELECT devicename, updatetimeDay, courtuuid, COUNT(DISTINCT rowkey) as countNum FROM device_access GROUP BY devicename, updatetimeDay, courtuuid")
    writeToMysql("cbox_smartvillage_v0.1", "device_access_devicetype_result", resultDf, mode)

    //凭证类型趋势
    resultDf = sparkSession.sql("SELECT description, updatetimeDay, courtuuid, COUNT(DISTINCT rowkey) as countNum FROM device_access GROUP BY description, updatetimeDay, courtuuid")
    writeToMysql("cbox_smartvillage_v0.1", "device_access_description_result", resultDf, mode)

    //设备通行时间分布
    resultDf = sparkSession.sql("SELECT devicename, updatetimeDay, courtuuid, COUNT(DISTINCT rowkey) as countNum FROM device_access GROUP BY devicename, updatetimeDay, courtuuid")
    writeToMysql("cbox_smartvillage_v0.1", "device_access_updatetime_result", resultDf, mode)
  }

  //  def executeDeviceAccess(sparkSession: SparkSession): Unit = {
  //    val sql = "SELECT rowkey, userid, usertype, devicename, updatetime, description from community.community_deviceacessdetail WHERE userid!='' AND usertype!='' AND devicename!='' AND updatetime!='' AND description!=''"
  //    val df = sparkSession.sql(sql)
  //    df.cache().createOrReplaceTempView("device_access")
  //    //    df.show()
  //
  //    sparkSession.udf.register("userTypeConverter", (value: Int) => {
  //      try {
  //        if (value == 1) {
  //          "业主"
  //        } else if (value == 2) {
  //          "家属"
  //        } else if (value == 3) {
  //          "租客"
  //        } else if (value == 4) {
  //          "访客"
  //        } else if (value == 5) {
  //          "物业"
  //        } else if (value == 6) {
  //          "快递"
  //        } else if (value == 7) {
  //          "外卖"
  //        } else {
  //          "其他"
  //        }
  //      } catch {
  //        case e: Exception => "其他"
  //      }
  //    })
  //
  //    sparkSession.udf.register("updatetimeLevel", (str: String) => {
  //      try {
  //        val hour: Int = str.split(" ")(1).split(":")(0).toInt
  //        if (hour >= 0 && hour < 8) {
  //          "0-8点"
  //        } else if (hour >= 8 && hour < 10) {
  //          "8-10点"
  //        } else if (hour >= 10 && hour < 12) {
  //          "10-12点"
  //        } else if (hour >= 12 && hour < 14) {
  //          "12-14点"
  //        } else if (hour >= 14 && hour < 18) {
  //          "14-18点"
  //        } else if (hour >= 18 && hour < 22) {
  //          "18-22点"
  //        } else if (hour >= 22 && hour < 24) {
  //          "22-34点"
  //        } else {
  //          "其他"
  //        }
  //      } catch {
  //        case e: Exception => "其他"
  //      }
  //    })
  //
  //    sparkSession.udf.register("timeDay", (str: String) => {
  //      try {
  //        str.split(" ")(0)
  //      } catch {
  //        case e: Exception => "其他"
  //      }
  //    })
  //
  //    val tempResult = sparkSession.sql("SELECT rowkey, userid, userTypeConverter(usertype) usertype, devicename, updatetimeLevel(updatetime) updatetimeLevel, timeDay(updatetime) updatetimeDay, description from device_access")
  //    tempResult.createOrReplaceTempView("device_access")
  //    tempResult.show()
  //
  //
  //    //人员类型趋势
  //    var resultDf = sparkSession.sql("SELECT usertype, updatetimeDay, COUNT(DISTINCT rowkey) as countNum FROM device_access GROUP BY usertype, updatetimeDay")
  //    writeToMysql("cbox_smartvillage_v0.1", "device_access_usertype_result", resultDf)
  //
  //    //设备类型趋势
  //    resultDf = sparkSession.sql("SELECT devicename, updatetimeDay, COUNT(DISTINCT rowkey) as countNum FROM device_access GROUP BY devicename, updatetimeDay")
  //    writeToMysql("cbox_smartvillage_v0.1", "device_access_devicetype_result", resultDf)
  //
  //    //凭证类型趋势
  //    resultDf = sparkSession.sql("SELECT description, updatetimeDay, COUNT(DISTINCT rowkey) as countNum FROM device_access GROUP BY description, updatetimeDay")
  //    writeToMysql("cbox_smartvillage_v0.1", "device_access_description_result", resultDf)
  //
  //    //设备通行时间分布
  //    resultDf = sparkSession.sql("SELECT devicename, updatetimeDay, COUNT(DISTINCT rowkey) as countNum FROM device_access GROUP BY devicename, updatetimeDay")
  //    writeToMysql("cbox_smartvillage_v0.1", "device_access_updatetime_result", resultDf)
  //  }

  def executePayment(sparkSession: SparkSession, mode: Int): Unit = {
    sparkSession.udf.register("payment_type_name", (value: Int) => {
      try {
        if (value == 1) {
          "月保停车"
        } else if (value == 2) {
          "固定车位"
        } else if (value == 3) {
          "物业缴费"
        } else if (value == 4 || value == 6) {
          "临时停车"
        } else if (value == 5) {
          "预缴"
        } else {
          "其他"
        }
      } catch {
        case e: Exception => "其他"
      }
    })

    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())

    var sql: String = ""
    if (mode == 0) {
      //全量更新
      sql = "SELECT left(cast(r.pay_time as VARCHAR), 10) as pay_date, o.payment_type, o.court_uuid, count(*) as total_times " +
        "FROM fm.fm_payment_record as r " +
        "INNER JOIN fm.fm_payment_order as o on o.out_trade_no = r.out_trade_no " +
        "GROUP BY pay_date, o.payment_type, o.court_uuid"
    } else {
      //增量更新，只拉取昨日数据
      sql = "SELECT left(cast(r.pay_time as VARCHAR), 10) as pay_date, o.payment_type, o.court_uuid, count(*) as total_times " +
        "FROM fm.fm_payment_record as r " +
        "INNER JOIN fm.fm_payment_order as o on o.out_trade_no = r.out_trade_no " +
        "where left(cast(r.pay_time as VARCHAR), 10) = '" + yesterday + "' " +
        "GROUP BY pay_date, o.payment_type, o.court_uuid"
    }

    var df = readFromPgsql(sparkSession, "hdsc_db", sql)
    df.cache().createOrReplaceTempView("payment_times")

    //业主App缴费笔数
    var resultDf = sparkSession.sql("SELECT pay_date, court_uuid, payment_type_name(payment_type) as payment_type, total_times from payment_times")
    writeToMysql("cbox_smartvillage_v0.1", "payment_times_result", resultDf, mode)

    if (mode == 0) {
      sql = "SELECT left(cast(r.pay_time as VARCHAR), 10) as pay_date, o.payment_type, o.court_uuid, cast(SUM(cast(total_amount as FLOAT8)) AS decimal(10,2)) as total_money " +
        "FROM fm.fm_payment_record as r " +
        "INNER JOIN fm.fm_payment_order as o on o.out_trade_no = r.out_trade_no " +
        "GROUP BY pay_date, o.payment_type, o.court_uuid"
    } else {
      sql = "SELECT left(cast(r.pay_time as VARCHAR), 10) as pay_date, o.payment_type, o.court_uuid, cast(SUM(cast(total_amount as FLOAT8)) AS decimal(10,2)) as total_money " +
        "FROM fm.fm_payment_record as r " +
        "INNER JOIN fm.fm_payment_order as o on o.out_trade_no = r.out_trade_no " +
        "where left(cast(r.pay_time as VARCHAR), 10) = '" + yesterday + "' " +
        "GROUP BY pay_date, o.payment_type, o.court_uuid"
    }

    df = readFromPgsql(sparkSession, "hdsc_db", sql)
    df.cache().createOrReplaceTempView("payment_money")

    //业主App缴费金额
    resultDf = sparkSession.sql("SELECT pay_date, court_uuid, payment_type_name(payment_type) as payment_type, total_money from payment_money")
    writeToMysql("cbox_smartvillage_v0.1", "payment_money_result", resultDf, mode)
  }

  //  def executePayment(sparkSession: SparkSession): Unit = {
  //    val url = "jdbc:postgresql://10.101.70.169:5432/hdsc_db"
  //    val prop = new Properties()
  //    prop.put("user", "hdsc_postgres")
  //    prop.put("password", "hdsc_postgres")
  //    prop.put("driver", "org.postgresql.Driver")
  //
  //    sparkSession.udf.register("payment_type_name", (value: Int) => {
  //      try {
  //        if (value == 1) {
  //          "月保停车"
  //        } else if (value == 2) {
  //          "固定车位"
  //        } else if (value == 3) {
  //          "物业缴费"
  //        } else if (value == 4 || value == 6) {
  //          "临时停车"
  //        } else if (value == 5) {
  //          "预缴"
  //        } else {
  //          "其他"
  //        }
  //      } catch {
  //        case e: Exception => "其他"
  //      }
  //    })
  //
  //    var sql = "SELECT left(cast(r.pay_time as VARCHAR), 10) as pay_date, o.payment_type, count(*) as total_times FROM fm.fm_payment_record as r INNER JOIN fm.fm_payment_order as o on o.out_trade_no = r.out_trade_no GROUP BY pay_date, o.payment_type"
  //    var df = sparkSession.read.jdbc(url, s"(${sql}) t", prop)
  //    df.cache().createOrReplaceTempView("payment_times")
  //
  //    //业主App缴费笔数
  //    var resultDf = sparkSession.sql("SELECT pay_date, payment_type_name(payment_type) as payment_type, total_times from payment_times")
  //    writeToMysql("cbox_smartvillage_v0.1", "payment_times_result", resultDf)
  //
  //    sql = "SELECT left(cast(r.pay_time as VARCHAR), 10) as pay_date, o.payment_type, cast(SUM(cast(total_amount as FLOAT8)) AS decimal(10,2)) as total_money FROM fm.fm_payment_record as r INNER JOIN fm.fm_payment_order as o on o.out_trade_no = r.out_trade_no GROUP BY pay_date, o.payment_type"
  //    df = sparkSession.read.jdbc(url, s"(${sql}) t", prop)
  //    df.cache().createOrReplaceTempView("payment_money")
  //
  //    //业主App缴费金额
  //    resultDf = sparkSession.sql("SELECT pay_date, payment_type_name(payment_type) as payment_type, total_money from payment_money")
  //    writeToMysql("cbox_smartvillage_v0.1", "payment_money_result", resultDf)
  //  }

  def executeBaseCourt(sparkSession: SparkSession, mode: Int): Unit = {
    var sql: String = ""

    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())

    if (mode == 0) {
      //全量更新
      sql = "SELECT uuid, name, province, city, district, left(cast(update_time as VARCHAR), 10) as update_time " +
        "FROM mdc.base_court"
    } else {
      //增量更新，只拉取昨日数据
      sql = "SELECT uuid, name, province, city, district, left(cast(update_time as VARCHAR), 10) as update_time " +
        "FROM mdc.base_court " +
        "WHERE left(cast(update_time as VARCHAR), 10) = '" + yesterday + "'"
    }

    val df = readFromPgsql(sparkSession, "hdsc_db", sql)
    writeToMysql("cbox_smartvillage_v0.1", "base_court", df, mode)
  }

  def writeToMysql(db: String, table: String, data: DataFrame, mode: Int): Unit = {
    val url = "jdbc:mysql://10.101.71.42:3306/" + db + "?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop.put("password", "hd123456")

    if (mode == 0) {
      //全量更新
      data.write.mode(SaveMode.Overwrite).jdbc(url, table, prop)
    } else {
      //增量更新
      data.write.mode(SaveMode.Append).jdbc(url, table, prop)
    }
  }

  def readFromMysql(sparkSession: SparkSession, db: String, sqlText: String): DataFrame = {
    val url = "jdbc:mysql://10.101.71.42:3306/" + db + "?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "hd123456")
    prop.put("driver", "com.mysql.jdbc.Driver")

    val df = sparkSession.read.jdbc(url, s"(${sqlText}) t", prop)
    return df
  }

  def readFromPgsql(sparkSession: SparkSession, db: String, sqlText: String): DataFrame = {
    val url = "jdbc:postgresql://10.101.70.169:5432/" + db
    val prop = new Properties()
    prop.put("user", "hdsc_postgres")
    prop.put("password", "hdsc_postgres")
    prop.put("driver", "org.postgresql.Driver")

    var df = sparkSession.read.jdbc(url, s"(${sqlText}) t", prop)
    return df
  }
}
