package com.eda.spark.sql.smartvillage

import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.DataTypes
import java.util.{Calendar, Date, Properties}

import com.eda.spark.utils.DBUtil
import org.apache.spark.rdd.RDD

object HiveExecutor extends Serializable {
  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).enableHiveSupport().getOrCreate()
    //    val sqlContext = SparkSession.builder().appName(this.getClass.getSimpleName).enableHiveSupport().getOrCreate()
    sqlContext.sparkContext.setLogLevel("WARN")

    val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
    var syncMode = 0

    if (today > "2020-02-25") { //上线发布日期
      syncMode = 1 //增量更新
    }

    executeICCard(sqlContext, syncMode)
    executeCarParking(sqlContext, syncMode)
//    executeDeviceAuth(sqlContext, syncMode)
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
        "AND cardstatustext !='' limit 100"
    } else {
      //增量更新，只拉取昨日数据
      sql = "SELECT rowkey, cardtype, owneruuid, ownertypetext, courtid, cardstatustext, updatetime " +
        "FROM community.community_idcarddetail " +
        "WHERE cardtype!='' " +
        "AND owneruuid!='' " +
        "AND ownertypetext !='' " +
        "AND courtid !='' " +
        "AND cardstatustext !='' " +
        "AND updatetime BETWEEN from_unixtime(unix_timestamp()-1*60*60*24, 'yyyy-MM-dd') AND from_unixtime(unix_timestamp(), 'yyyy-MM-dd') limit 100"
    }

    val df = sparkSession.sql(sql)
    df.cache().createOrReplaceTempView("ic_card")

    //IC持卡人分布
    var resultDf = sparkSession.sql("SELECT cardtype, ownertypetext, courtid, COUNT(DISTINCT rowkey) as countNum FROM ic_card GROUP BY cardtype, ownertypetext, courtid")

    val dbUtil = new DBUtil()
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "ic_card_user_result", resultDf, mode)

    //IC状态分布
    resultDf = sparkSession.sql("SELECT cardtype, cardstatustext, courtid, COUNT(DISTINCT rowkey) as countNum FROM ic_card GROUP BY cardtype, cardstatustext, courtid")
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "ic_card_status_result", resultDf, mode)
  }

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
        "AND payedmoney!='' limit 100"
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
        "AND outtime BETWEEN from_unixtime(unix_timestamp()-1*60*60*24, 'yyyy-MM-dd') AND from_unixtime(unix_timestamp(), 'yyyy-MM-dd') limit 100"
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
          "0-08点"
        } else if (hour >= 8 && hour < 10) {
          "08-10点"
        } else if (hour >= 10 && hour < 12) {
          "10-12点"
        } else if (hour >= 12 && hour < 14) {
          "12-14点"
        } else if (hour >= 14 && hour < 18) {
          "14-18点"
        } else if (hour >= 18 && hour < 22) {
          "18-22点"
        } else if (hour >= 22 && hour < 24) {
          "22-24点"
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
        if (str.length > 7) {
          "新能源"
        } else {
          "非新能源"
        }
      } catch {
        case e: Exception => "其他"
      }
    })

    sparkSession.udf.register("carSubEnergy", (str: String) => {
      try {
        if (str.length > 7) {
          if (str.substring(2, 3) == "D") {
            "纯电动"
          } else if (str.substring(2, 3) == "F") {
            "混合动力"
          } else {
            "其他"
          }
        } else {
          "非新能源"
        }
      } catch {
        case e: Exception => "其他"
      }
    })

    val tempResult = sparkSession.sql("SELECT rowkey, carnum, carEnergy(carnum) car_energy, carSubEnergy(carnum) car_sub_energy, stopTimeLevel(stopTime) stopTimeLevel, intimeLevel(intime) intimeLevel, timeDay(outtime) outtime, entermodetext, exitmodetext, carporttypetext, consumemoney, payedmoney, courtid FROM car_parking")
    tempResult.createOrReplaceTempView("car_parking")
    tempResult.show()

    //机动车停车时长分布
    var resultDf = sparkSession.sql("SELECT stopTimeLevel, courtid, COUNT(DISTINCT rowkey) as countNum FROM car_parking GROUP BY stopTimeLevel, courtid")

    val dbUtil = new DBUtil()
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "car_parking_stoptime_result", resultDf, mode)

    //机动车入场时间分布
    resultDf = sparkSession.sql("SELECT intimeLevel, courtid, COUNT(DISTINCT rowkey) as countNum FROM car_parking GROUP BY intimeLevel, courtid")
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "car_parking_intime_result", resultDf, mode)

    //机动车出场开闸方式
    resultDf = sparkSession.sql("SELECT exitmodetext, courtid, COUNT(DISTINCT rowkey) as countNum FROM car_parking GROUP BY exitmodetext, courtid")
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "car_parking_exitmode_result", resultDf, mode)

    //车辆类型比例
    resultDf = sparkSession.sql("SELECT carporttypetext, courtid, COUNT(DISTINCT rowkey) as countNum FROM car_parking GROUP BY carporttypetext, courtid")
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "car_parking_parkingtype_result", resultDf, mode)

    //车辆缴费
    resultDf = sparkSession.sql("SELECT outtime, courtid, SUM(payedmoney) as payedmoney_total, SUM(consumemoney) as consumemoney_total FROM car_parking GROUP BY outtime, courtid")
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "car_parking_pay_result", resultDf, mode)

    //新能源车通行
    resultDf = sparkSession.sql("SELECT outtime, courtid, carnum, car_energy, car_sub_energy, COUNT(DISTINCT rowkey) as countNum FROM car_parking GROUP BY car_energy, outtime, courtid, carnum, car_sub_energy")
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "car_parking_energy_result", resultDf, mode)
  }


  def executeDeviceAuth(sparkSession: SparkSession, mode: Int): Unit = {
    var sql: String = ""

    if (mode == 0) {
      //全量更新
      sql = "SELECT rowkey, usertype, devicename, subdevicename, credencetype, courtuuid, updatetime " +
        "FROM community.community_deviceauthdetail " +
        "WHERE usertype!='' " +
        "AND devicename like '[%' " +
        "AND courtuuid!='' " +
        "AND credencetype like '[%'  limit 100"
    } else {
      //增量更新，只拉取昨日数据
      sql = "SELECT rowkey, usertype, devicename, subdevicename, credencetype, courtuuid, updatetime " +
        "FROM community.community_deviceauthdetail " +
        "WHERE usertype!='' " +
        "AND devicename like '[%' " +
        "AND courtuuid!='' " +
        "AND credencetype like '[%' " +
        "AND updatetime BETWEEN from_unixtime(unix_timestamp()-1*60*60*24, 'yyyy-MM-dd') AND from_unixtime(unix_timestamp(), 'yyyy-MM-dd') limit 100"
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

    val tempResult = sparkSession.sql("SELECT rowkey, courtuuid, userTypeConverter(usertype) usertype, devicename, subdevicename, credencetype FROM device_auth")
    tempResult.createOrReplaceTempView("device_auth")
    tempResult.show()


    //人员类型比例
    var resultDf = sparkSession.sql("SELECT usertype, courtuuid, credencetype, COUNT(DISTINCT rowkey) as countNum FROM device_auth GROUP BY usertype, credencetype, courtuuid")
    val dbUtil = new DBUtil()
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "device_auth_usertype_result", resultDf, mode)

    //设备类型比例
    //    resultDf = sparkSession.sql("SELECT devicename, courtuuid, COUNT(DISTINCT rowkey) as countNum FROM device_auth GROUP BY devicename, courtuuid")
    //    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "device_auth_devicetype_result", resultDf, mode)


    //凭证类型比例
    resultDf = sparkSession.sql("SELECT credencetype, courtuuid, COUNT(DISTINCT rowkey) as countNum FROM device_auth GROUP BY credencetype, courtuuid")
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "device_auth_credencetype_result", resultDf, mode)
  }


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
        "AND description!='' limit 100"
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
        "AND updatetime BETWEEN from_unixtime(unix_timestamp()-1*60*60*24, 'yyyy-MM-dd') AND from_unixtime(unix_timestamp(), 'yyyy-MM-dd') limit 100"
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
          "0-08点"
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
          "22-24点"
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
    val dbUtil = new DBUtil()
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "device_access_usertype_result", resultDf, mode)

    //设备类型趋势
    resultDf = sparkSession.sql("SELECT devicename, updatetimeDay, courtuuid, COUNT(DISTINCT rowkey) as countNum FROM device_access GROUP BY devicename, updatetimeDay, courtuuid")
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "device_access_devicetype_result", resultDf, mode)

    //凭证类型趋势
    resultDf = sparkSession.sql("SELECT description, updatetimeDay, courtuuid, COUNT(DISTINCT rowkey) as countNum FROM device_access GROUP BY description, updatetimeDay, courtuuid")
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "device_access_description_result", resultDf, mode)

    //设备通行时间分布
    resultDf = sparkSession.sql("SELECT devicename, updatetimeDay, courtuuid, COUNT(DISTINCT rowkey) as countNum FROM device_access GROUP BY devicename, updatetimeDay, courtuuid")
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "device_access_updatetime_result", resultDf, mode)
  }


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
        "GROUP BY pay_date, o.payment_type, o.court_uuid limit 100"
    } else {
      //增量更新，只拉取昨日数据
      sql = "SELECT left(cast(r.pay_time as VARCHAR), 10) as pay_date, o.payment_type, o.court_uuid, count(*) as total_times " +
        "FROM fm.fm_payment_record as r " +
        "INNER JOIN fm.fm_payment_order as o on o.out_trade_no = r.out_trade_no " +
        "where left(cast(r.pay_time as VARCHAR), 10) = '" + yesterday + "' " +
        "GROUP BY pay_date, o.payment_type, o.court_uuid limit 100"
    }

    val dbUtil = new DBUtil()
    var df = dbUtil.readFromPgsql(sparkSession, "10.101.70.169", 5432, "hdsc_db", "hdsc_postgres", "hdsc_postgres", sql)
    df.cache().createOrReplaceTempView("payment_times")

    //业主App缴费笔数
    var resultDf = sparkSession.sql("SELECT pay_date, court_uuid, payment_type_name(payment_type) as payment_type, total_times from payment_times")
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "payment_times_result", resultDf, mode)

    if (mode == 0) {
      sql = "SELECT left(cast(r.pay_time as VARCHAR), 10) as pay_date, o.payment_type, o.court_uuid, cast(SUM(cast(total_amount as FLOAT8)) AS decimal(10,2)) as total_money " +
        "FROM fm.fm_payment_record as r " +
        "INNER JOIN fm.fm_payment_order as o on o.out_trade_no = r.out_trade_no " +
        "GROUP BY pay_date, o.payment_type, o.court_uuid limit 100"
    } else {
      sql = "SELECT left(cast(r.pay_time as VARCHAR), 10) as pay_date, o.payment_type, o.court_uuid, cast(SUM(cast(total_amount as FLOAT8)) AS decimal(10,2)) as total_money " +
        "FROM fm.fm_payment_record as r " +
        "INNER JOIN fm.fm_payment_order as o on o.out_trade_no = r.out_trade_no " +
        "where left(cast(r.pay_time as VARCHAR), 10) = '" + yesterday + "' " +
        "GROUP BY pay_date, o.payment_type, o.court_uuid limit 100"
    }

    df = dbUtil.readFromPgsql(sparkSession, "10.101.70.169", 5432, "hdsc_db", "hdsc_postgres", "hdsc_postgres", sql)
    df.cache().createOrReplaceTempView("payment_money")

    //业主App缴费金额
    resultDf = sparkSession.sql("SELECT pay_date, court_uuid, payment_type_name(payment_type) as payment_type, total_money from payment_money")
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "payment_money_result", resultDf, mode)
  }

  def executeBaseCourt(sparkSession: SparkSession, mode: Int): Unit = {
    var sql: String = ""

    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())

    if (mode == 0) {
      //全量更新
      sql = "SELECT uuid, name, province, city, district, left(cast(update_time as VARCHAR), 10) as update_time " +
        "FROM mdc.base_court limit 100"
    } else {
      //增量更新，只拉取昨日数据
      sql = "SELECT uuid, name, province, city, district, left(cast(update_time as VARCHAR), 10) as update_time " +
        "FROM mdc.base_court " +
        "WHERE left(cast(update_time as VARCHAR), 10) = '" + yesterday + "' limit 100"
    }

    val dbUtil = new DBUtil()
    var df = dbUtil.readFromPgsql(sparkSession, "10.101.70.169", 5432, "hdsc_db", "hdsc_postgres", "hdsc_postgres", sql)
    df.cache().createOrReplaceTempView("base_court")

    sparkSession.udf.register("province_check", (str: String) => {
      try {
        if (str.trim.length == 0) {
          "其他"
        } else if (str == "深圳") {
          "广东省"
        } else if (!str.endsWith("区") && !str.endsWith("省") && !str.endsWith("市")
          && !str.contains("北京") && !str.contains("上海")
          && !str.contains("重庆") && !str.contains("天津")
          && !str.contains("内蒙古") && !str.contains("宁夏")
          && !str.contains("广西") && !str.contains("西藏")
          && !str.contains("新疆")) {
          str + "省"
        } else if (str.contains("北京")) {
          "北京市"
        } else if (str.contains("上海")) {
          "上海市"
        } else if (str.contains("天津")) {
          "天津市"
        } else if (str.contains("重庆")) {
          "重庆市"
        } else if (str.contains("内蒙古")) {
          "内蒙古自治区"
        } else if (str.contains("新疆")) {
          "新疆维吾尔自治区"
        } else if (str.contains("广西")) {
          "广西壮族自治区"
        } else if (str.contains("宁夏")) {
          "宁夏回族自治区"
        } else if (str.contains("西藏")) {
          "西藏自治区"
        } else {
          str
        }
      } catch {
        case e: Exception => "其他"
      }
    })

    val resultDf = sparkSession.sql("SELECT uuid, name, province_check(province) province, city, district, update_time from base_court")
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "base_court", resultDf, mode)
  }

  def executeErpOrg(sparkSession: SparkSession, mode: Int): Unit = {
    var sql: String = ""

    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())

    if (mode == 0) {
      //全量更新
      sql = "SELECT uuid, name, parent_uuid, description, court_uuid, court_name, org_type, left(cast(update_time as VARCHAR), 10) as update_time " +
        "FROM mdc.erp_org where delete_flag = 1 limit 100"
    } else {
      //增量更新，只拉取昨日数据
      sql = "SELECT uuid, name, parent_uuid, description, court_uuid, court_name, org_type, left(cast(update_time as VARCHAR), 10) as update_time " +
        "FROM mdc.erp_org where delete_flag = 1 limit 100 " +
        "WHERE left(cast(update_time as VARCHAR), 10) = '" + yesterday + "'"
    }

    sparkSession.udf.register("org_type_trans", (str: String) => {
      try {
        if (str == "1") {
          "集团"
        } else if (str == "2") {
          "分公司"
        } else if (str == "3") {
          "项目"
        } else {
          "其他"
        }
      } catch {
        case e: Exception => "其他"
      }
    })

    val dbUtil = new DBUtil()
    var df = dbUtil.readFromPgsql(sparkSession, "10.101.70.169", 5432, "hdsc_db", "hdsc_postgres", "hdsc_postgres", sql)
    df.cache().createOrReplaceTempView("erp_org")

    val resultDf = sparkSession.sql("SELECT uuid, name, parent_uuid, description, court_uuid, court_name, org_type_trans(org_type) org_type, update_time from erp_org")
    dbUtil.writeToMysql("10.101.71.42", 3306, "cbox_smartvillage_v0.1", "root", "hd123456", "erp_org", resultDf, mode)
  }
}
