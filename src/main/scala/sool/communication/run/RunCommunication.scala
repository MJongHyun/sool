package sool.communication.run

import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager

import sool.common.check_args.CheckArgsRun

import sool.communication.get_communication_hj.RunHjCommunication
import sool.communication.get_communication_dk.RunDkCommunication
import sool.communication.get_communication_hk.RunHkCommunication

object RunCommunication {
  val spark = SparkSession.builder().appName(name = "communication").getOrCreate()
  val logger = LogManager.getLogger("myLogger")

  def main(args: Array[String]) = {
    logger.info("[appName=communication] [function=main] [runStatus=start] [message=start]")

    val targetCompany = args(0)
    val checkExistTargetBool = new CheckArgsRun().checkCommunicationArgsVals(targetCompany)

    if (checkExistTargetBool == false) {
      logger.error("[appName=communication] [function=main] [runStatus=error] [message=main parameter error]")
    } else {
      if (targetCompany == "HJ") {
        val runHjCommunicationObj = new RunHjCommunication(spark)
        runHjCommunicationObj.runHjCommunication()
        runHjCommunicationObj.runHj2Communication()
      } else if (targetCompany == "DK") {
        new RunDkCommunication(spark).runDkCommunication()
      } else if (targetCompany == "HK") {
       new RunHkCommunication(spark).runHkCommunication()
      }
    }

    logger.info("[appName=communication] [function=main] [runStatus=end] [message=end]")
  }
}
