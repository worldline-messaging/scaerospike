package com.tapad.aerospike

import com.aerospike.client.async.AsyncClient
import com.aerospike.client.listener.{DeleteListener, RecordArrayListener, RecordListener, RecordSequenceListener, WriteListener}
import com.aerospike.client._
import com.aerospike.client.policy._

import scala.concurrent.{ExecutionContext, Future, Promise}
import com.aerospike.client.{AerospikeException, Key}

import scala.collection.{breakOut}
import scala.collection.generic.CanBuildFrom
import java.util.{List => jList}

trait AsRecordListener[K, V]  {
  @throws[AerospikeException]
  def onRecord(key: K, record: Map[String,V])

  def onSuccess()

  def onFailure(exception: AerospikeException)
}

/**
 * Operations on an Aerospike set in a namespace.
 *
 * @tparam K the key type
 * @tparam V the value type. If you have bins / sets with different types, use AnyRef and cast.
 */
trait AsSetOps[K, V] {
  /**
   * Gets the default bin for a single key.
   */
  def get(key: K, bin: String = ""): Future[Option[V]]

  /**
   * Gets multiple bins for a single key.
   */
  def getBins(key: K, bins: Seq[String]) : Future[Map[String, V]]

  /**
   * Gets the default bin for multiple keys.
   */
  def multiGet(keys: Seq[K], bin: String = ""): Future[Map[K, Option[V]]]

  /**
   * Gets multiple bins for a single key.
   */
  def multiGetBins(keys: Seq[K], bins: Seq[String]): Future[Map[K, Map[String, V]]]

  def multiGetBinsL(keys: Seq[K], bins: Seq[String]): Future[List[(K, Map[String, V])]]
  
  /**
   * Put a value into a key.
   */
  def put(key: K, value: V, bin: String = "", customTtl: Option[Int] = None): Future[Unit]

  /**
   * Put values into multiple bins for a key.
   */
  def putBins(key: K, values: Map[String, V], customTtl: Option[Int] = None) : Future[Unit]

  /**
   * Just change the generation and/or the TTL
   * @param key
   * @param customTtl
   * @return
   */
  def touch (key:K,customTtl: Option[Int] = None) : Future[Unit]

  def getandtouch(key: K,
               customTtl: Option[Int] = None): Future[Map[String, V]]
    /**
   * Delete a key.
   */
  def delete(key: K, bin: String = "") : Future[Unit]
  
  def scanRecords(bins: Seq[String], listener: AsRecordListener[K,V], priority:Priority = Priority.DEFAULT): Unit

}

/**
 * Represents a set in a namespace tied to a specific client.
 */
private[aerospike] class AsSet[K, V](private final val client: AsyncClient,
                                     namespace: String,
                                     set: String,
                                     readSettings: ReadSettings,
                                     writeSettings: WriteSettings)
                                    (implicit keyGen: KeyGenerator[K], valueMapping: ValueMapping[V], executionContext: ExecutionContext) extends AsSetOps[K, V] {

  private final val queryPolicy = readSettings.buildQueryPolicy()
  private final val batchPolicy = readSettings.buildBatchPolicy()
  private final val writePolicy = writeSettings.buildWritePolicy()

  val genKey = keyGen(namespace, set, (_: K))

  private def extractSingleBin(bin: String, record: Record): Option[V] = record match {
    case null => None
    case rec => Option(valueMapping.fromStoredObject(rec.getValue(bin)))
  }

  private def extractMultiBin(record: Record): Map[String, V] = record match {
    case null => Map.empty
    case rec => {
      val result = Map.newBuilder[String, V]
      if(rec.bins!=null) {
	      val iter = rec.bins.entrySet().iterator()
	      while (iter.hasNext) {
	        val bin = iter.next()
	        val obj = bin.getValue
	        if (obj != null) result += bin.getKey -> valueMapping.fromStoredObject(obj)
	      }
      }
      result.result()
    }
  }

  private[aerospike] def query[R](policy: QueryPolicy,
                                  key: Key,
                                  bins: Seq[String] = Seq.empty, extract: Record => R): Future[R] = {
    val result = Promise[R]()
    val listener = new RecordListener {
      def onFailure(exception: AerospikeException): Unit = result.failure(exception)

      def onSuccess(key: Key, record: Record): Unit = result.success(extract(record))
    }
    try {
      if (bins.isEmpty) client.get(policy, listener, key)
      else client.get(policy, listener, key, bins: _*)
    } catch {
      case e: Exception => result.failure(e)
    }
    result.future
  }

  
  private[aerospike] def multiQuery[T](policy: BatchPolicy,
                                       keys: Seq[Key],
                                       bins: Seq[String],
                                       extract: Record => T): Future[Map[K, T]] = {
    val result = Promise[Map[K, T]]()
    val listener = new RecordArrayListener {
      def onFailure(exception: AerospikeException): Unit = result.failure(exception)

      def onSuccess(keys: Array[Key], records: Array[Record]): Unit = {
        var i = 0
        val size = keys.length
        val data = Map.newBuilder[K, T]
        while (i < size) {
          data += keys(i).userKey.getObject.asInstanceOf[K] -> extract(records(i))
          i += 1
        }
        result.success(data.result())
      }
    }
    try {
      if (bins.isEmpty) client.get(policy, listener, keys.toArray)
      else client.get(policy, listener, keys.toArray, bins: _*)
    } catch {
      case e: Exception => result.failure(e)
    }
    result.future
  }

  private[aerospike] def multiQueryL[C[_],T](policy: BatchPolicy,
                                       keys: Seq[Key],
                                       bins: Seq[String],
                                       extract: Record => T)
             (implicit cbf: CanBuildFrom[C[(K, T)], (K, T), C[(K, T)]])
             : Future[C[(K, T)]] = {
    val result = Promise[C[(K, T)]]()
    val listener = new RecordArrayListener {
      def onFailure(exception: AerospikeException): Unit = result.failure(exception)

      def onSuccess(keys: Array[Key], records: Array[Record]): Unit = {
        var i = 0
        val size = keys.length
        val data = cbf()
        while (i < size) {
          val key = keys(i).userKey.getObject.asInstanceOf[K]
          val value = extract(records(i))
          data += ((key , value))
          i += 1
        }
        result.success(data.result())
      }
    }
    try {
      if (bins.isEmpty) client.get(policy, listener, keys.toArray)
      else client.get(policy, listener, keys.toArray, bins: _*)
    } catch {
      case e: Exception => result.failure(e)
    }
    result.future
  }

  def scanRecords(bins: Seq[String], listener: AsRecordListener[K,V], priority:Priority = Priority.DEFAULT): Unit = {
    val policy = new ScanPolicy()
    policy.concurrentNodes = true
    policy.priority = priority
    policy.includeBinData = bins !=null && bins.size > 0

    val asListener = new RecordSequenceListener {
      @throws[AerospikeException]
      def onRecord(key: Key, record: Record): Unit = {
        val row = extractMultiBin(record)
        val k = key.userKey.getObject.asInstanceOf[K]
        listener.onRecord(k, row)
      }

      /**
        * This method is called when the asynchronous batch get or scan command completes.
        */
      def onSuccess(): Unit = {
        listener.onSuccess()
      }

      /**
        * This method is called when an asynchronous batch get or scan command fails.
        *
        * @param exception error that occurred
        */
      def onFailure(exception: AerospikeException): Unit = {
        listener.onFailure(exception)
      }
    }
    client.scanAll(policy,asListener,namespace,set,bins: _*)

  }
    
  def get(key: K, bin: String = ""): Future[Option[V]] = {
    query(queryPolicy, genKey(key), bins = Seq(bin), extractSingleBin(bin, _))
  }

  def getBins(key: K, bins: Seq[String]): Future[Map[String, V]] = {
    query(queryPolicy, genKey(key), bins = bins, extractMultiBin)
  }

  def multiGet(keys: Seq[K], bin: String = ""): Future[Map[K, Option[V]]] = {
    multiQuery(batchPolicy, keys.map(genKey), bins = Seq(bin), extractSingleBin(bin, _))
  }

  def multiGetBins(keys: Seq[K], bins: Seq[String]): Future[Map[K, Map[String, V]]] = {
    multiQuery(batchPolicy, keys.map(genKey), bins, extractMultiBin)
  }

  def multiGetBinsL(keys: Seq[K], bins: Seq[String]): Future[List[(K, Map[String, V])]] = {
    multiQueryL[List,Map[String, V]](batchPolicy, keys.map(genKey), bins, extractMultiBin)
  }
  
  def put(key: K, value: V, bin: String = "", customTtl: Option[Int] = None): Future[Unit] = {
    putBins(key, Map(bin -> value), customTtl)
  }

  def putBins(key: K, values: Map[String, V], customTtl: Option[Int] = None) : Future[Unit] = {
    val policy = customTtl match {
      case None => writePolicy
      case Some(ttl) =>
        val p = writeSettings.buildWritePolicy()
        p.expiration = ttl
        p
    }
    val bins: Array[Bin] = values.map { case (binName, binValue) => new Bin(binName, valueMapping.toAerospikeValue(binValue)) }(breakOut)
    val result = Promise[Unit]()
    try {
      val listener = new WriteListener {
        def onFailure(exception: AerospikeException) { result.failure(exception) }

        def onSuccess(key: Key) { result.success(Unit) }
      }
      //println("putBins key="+genKey(key))
      client.put(policy, listener, genKey(key), bins: _*)
    } catch {
      case e: Exception => result.failure(e)
    }
    result.future
  }

  def touch (key:K,customTtl: Option[Int] = None) : Future[Unit] = {
    val policy = customTtl match {
      case None => writePolicy
      case Some(ttl) =>
        val p = writeSettings.buildWritePolicy()
        p.expiration = ttl
        p.commitLevel = CommitLevel.COMMIT_MASTER //optimization! will be replicated later
        p
    }
    val result = Promise[Unit]()
    try {
      val listener = new RecordListener() {
        def onFailure(exception: AerospikeException) { exception.printStackTrace();result.failure(exception) }
    	def onSuccess(key: Key, record: Record) { result.success(Unit) }
      }
      client.operate(policy, listener, genKey(key), Operation.touch)
    } catch {
      case e: Exception => e.printStackTrace();result.failure(e)
    }
    result.future                              		   
  }
  
  def getandtouch(key: K,
               customTtl: Option[Int] = None): Future[Map[String, V]] = {
    val policy = customTtl match {
      case None => writePolicy
      case Some(ttl) =>
        val p = writeSettings.buildWritePolicy()
        p.expiration = ttl
        p.commitLevel = CommitLevel.COMMIT_MASTER //optimization! will be replicated later
        p
    }
    val result = Promise[Map[String, V]]()
    val listener = new RecordListener {
      def onFailure(exception: AerospikeException): Unit = result.failure(exception)

      def onSuccess(key: Key, record: Record): Unit = result.success(extractMultiBin(record))
    }
    try {
      client.operate(policy, listener, genKey(key), Operation.touch, Operation.get)
    } catch {
      case e: Exception => result.failure(e)
    }
    result.future
  }

  def delete(key: K, bin: String = ""): Future[Unit] = {
    val result = Promise[Unit]()
    try {
      val listener = new DeleteListener {
        def onFailure(exception: AerospikeException) {
          result.failure(exception)
        }

        def onSuccess(key: Key, existed: Boolean) {
          result.success(Unit)
        }
      }
      client.delete(writePolicy, listener, genKey(key))
    } catch {
      case e: Exception => result.failure(e)
    }
    result.future
  }

}


