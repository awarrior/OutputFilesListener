import java.util.concurrent.LinkedBlockingQueue

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.{Logging, SparkConf, SparkFirehoseListener, Success}

import scala.collection.mutable

/**
  * As an extra listener to SparkListenerBus, constructor with one SparkConf parameter is needed.
  *
  * 1. map from stage to job
  * 2. collect task data in stage
  * 3. print output files metrics
  */
class OutputFilesListener(conf: SparkConf) extends SparkFirehoseListener with Logging {

  type JobId = Int
  type StageId = Int
  type TaskId = Long
  type StageAttemptId = Int
  type TaskOutputBytes = Long
  type TaskOutputRecords = Long

  class StageStatData {
    var outputBytes: Long = _
    var outputRecords: Long = _
    val taskData = new mutable.HashMap[TaskId, TaskMetrics]
    val taskOutputBytes = new mutable.HashMap[TaskId, TaskOutputBytes]
    val taskOutputRecords = new mutable.HashMap[TaskId, TaskOutputRecords]

    def hasOutput: Boolean = outputRecords > 0 | outputBytes > 0
  }

  private val stageIdToData = new mutable.HashMap[(StageId, StageAttemptId), StageStatData]()
  private val stageIdToJobId = new mutable.HashMap[StageId, JobId]()

  // event queue definition
  private val eventQueue = new LinkedBlockingQueue[SparkListenerEvent]()

  // function needed to realized in SparkFirehoseListener
  override def onEvent(event: SparkListenerEvent): Unit = {
    eventQueue.add(event)
  }

  // start one listener thread to handle events in the blocking queue
  private val listenerThread = new Thread("OutputFilesListenerThread") {
    setDaemon(true)

    override def run(): Unit = {
      logInfo(s"Listener startup")
      while (true) {
        val event = eventQueue.take()
        event match {
          case e: SparkListenerJobStart => statJobStart(e)
          case e: SparkListenerTaskEnd => statTaskEnd(e)
          case e: SparkListenerStageCompleted => statStageCompleted(e)
          case _ =>
        }
      }
    }
  }
  listenerThread.start()

  /**
    * statistics of JobStart event
    *
    * @param event
    */
  def statJobStart(event: SparkListenerJobStart): Unit = {
    event.stageIds.foreach(stageIdToJobId(_) = event.jobId)
  }

  /**
    * statistics of TaskEnd event
    *
    * @param event
    */
  def statTaskEnd(event: SparkListenerTaskEnd): Unit = {
    val info = event.taskInfo
    // stageAttemptId is used for speculation task
    if (info != null && event.stageAttemptId != -1) {
      // get task metrics according to whether task is successful or not
      val metrics = event.reason match {
        case Success =>
          Option(event.taskMetrics)
        case _ =>
          None
      }

      if (metrics.isDefined) {
        // get old metrics
        val taskId = info.taskId
        val stageStatData = stageIdToData.getOrElseUpdate((event.stageId, event.stageAttemptId), {
//          logWarning(s"$LABEL Task end for unknown stage ${event.stageId}")
          new StageStatData
        })
        val oldMetrics = stageStatData.taskData.get(taskId)

        // update bytes/records written metrics according to oldMetrics
        val bytesWrittenDelta = metrics.get.outputMetrics.map(_.bytesWritten).getOrElse(0L) - oldMetrics.flatMap(_.outputMetrics).map(_.bytesWritten).getOrElse(0L)
        stageStatData.outputBytes += bytesWrittenDelta
        val recordsWrittenDelta = metrics.get.outputMetrics.map(_.recordsWritten).getOrElse(0L) - oldMetrics.flatMap(_.outputMetrics).map(_.recordsWritten).getOrElse(0L)
        stageStatData.outputRecords += recordsWrittenDelta

        // update task info
        stageStatData.taskData(taskId) = metrics.get
        if (bytesWrittenDelta > 0) {
          stageStatData.taskOutputBytes.getOrElseUpdate(taskId, 0L)
          stageStatData.taskOutputBytes(taskId) += bytesWrittenDelta
        }
        if (recordsWrittenDelta > 0) {
          stageStatData.taskOutputRecords.getOrElseUpdate(taskId, 0L)
          stageStatData.taskOutputRecords(taskId) += recordsWrittenDelta
        }
      }
    }
  }

  /**
    * statistics of StageCompleted event
    *
    * @param event
    */
  def statStageCompleted(event: SparkListenerStageCompleted): Unit = {
    // get stage stat data
    val stage = event.stageInfo
    val stageStatData = stageIdToData.getOrElseUpdate((stage.stageId, stage.attemptId), {
//      logWarning(s"$LABEL Stage completed for unknown stage ${stage.stageId}")
      new StageStatData
    })

    if (stageStatData.hasOutput) {
      // mean/var calculation of task output bytes
      val bytesSize = stageStatData.taskOutputBytes.size
      val bytesMean = stageStatData.outputBytes * 1.0 / bytesSize
      val bytesVar = math.sqrt(stageStatData.taskOutputBytes.values.par.map(a => {
        val b = a - bytesMean
        b * b
      }).sum) / bytesSize
      // mean/var calculation of task output records
      val recordsSize = stageStatData.taskOutputRecords.size
      val recordsMean = stageStatData.outputRecords * 1.0 / recordsSize
      val recordsVar = math.sqrt(stageStatData.taskOutputRecords.values.par.map(a => {
        val b = a - recordsMean
        b * b
      }).sum) / recordsSize

      // output files metrics print
      logInfo(s"[metrics] appId:${conf.getAppId},jobId:${stageIdToJobId(stage.stageId)};" +
        f"bytes:${stageStatData.outputBytes},bytesFiles:$bytesSize,bytesMean:$bytesMean%.2f,bytesVar:$bytesVar%.2f;" +
        f"records:${stageStatData.outputRecords},recordsFiles:$recordsSize,recordsMean:$recordsMean%.2f,recordsVar:$recordsVar%.2f")
    }

    // clean cache
    stageIdToJobId.remove(stage.stageId)
    stageIdToData.remove((stage.stageId, stage.attemptId))
  }

}
