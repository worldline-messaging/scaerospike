package com.tapad.aerospike

import com.aerospike.client.async.{MaxCommandAction, AsyncClientPolicy}
import com.aerospike.client.policy.{WritePolicy, QueryPolicy}
import java.util.concurrent.ExecutorService
import com.aerospike.client.policy.CommitLevel

/**
 * Aerospike client settings.
 *
 * @param maxCommandsOutstanding the maximum number of outstanding commands before rejections will happen
 */
case class ClientSettings(maxCommandsOutstanding: Int = 500, selectorThreads: Int = 1, maxSocketIdle : Int = 14, taskThreadPool: ExecutorService = null, blockingMode: Boolean = false) {

  /**
   * @return a mutable policy object for the Java client.
   */
  private[aerospike] def buildClientPolicy() = {
    val p = new AsyncClientPolicy()
    p.asyncMaxCommandAction = if(blockingMode) MaxCommandAction.BLOCK else MaxCommandAction.REJECT 
    p.asyncMaxCommands      = maxCommandsOutstanding
    p.asyncSelectorThreads  = selectorThreads
    p.maxSocketIdle         = maxSocketIdle
    p.asyncTaskThreadPool   = taskThreadPool
    p
  }
}

object ClientSettings {
  val Default = ClientSettings()
}


case class ReadSettings(timeout: Int = 0, maxRetries: Int = 2, sleepBetweenRetries: Int = 500, maxConcurrentNodes: Int = 0) {
  private[aerospike] def buildQueryPolicy() = {
    val p = new QueryPolicy()
    p.timeout             = timeout
    p.maxRetries          = maxRetries
    p.sleepBetweenRetries = sleepBetweenRetries
    p.maxConcurrentNodes  = maxConcurrentNodes
    p
  }
}

object ReadSettings {
  val Default = ReadSettings()
}

case class WriteSettings(expiration: Int = 0, timeout: Int = 0, maxRetries: Int = 2, sleepBetweenRetries: Int = 500, sendKey: Boolean = true, commitAll: Boolean = true) {
  private[aerospike] def buildWritePolicy() = {
    val p = new WritePolicy()
    p.expiration = expiration
    p.timeout = timeout
    p.maxRetries = maxRetries
    p.sleepBetweenRetries = sleepBetweenRetries
    p.sendKey = sendKey
    if(commitAll) p.commitLevel = CommitLevel.COMMIT_ALL else p.commitLevel = CommitLevel.COMMIT_MASTER 
    p
  }
}

object WriteSettings {
  val Default = WriteSettings()
}

