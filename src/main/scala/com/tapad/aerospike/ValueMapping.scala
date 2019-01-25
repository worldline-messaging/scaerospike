package com.tapad.aerospike

import com.aerospike.client.Value
import com.aerospike.client.Value.{ByteSegmentValue, BytesValue, ListValue}
import java.util.{List ⇒ jList}

/*
 * Defines a mapping to the Aerospike "Values" and from the stored object to a representation the client can work with.
 * Note that this should not be used to do deserialization but just to map to some common underlying format, since it will be applied
 * to all bins in a multi bin query and would thus potentially have to be able to handle various formats and return a generic supertype.
 * It will also be run on the selector threads, so don't do too much work here.
 */
trait ValueMapping[T] {
  def toAerospikeValue(t: T): Value
  def fromStoredObject(v: Object): T
}

object DefaultValueMappings {
  implicit val byteArrayMapping = new ValueMapping[Array[Byte]] {
    def toAerospikeValue(arr: Array[Byte]) = new BytesValue(arr)
    def fromStoredObject(v: Object): Array[Byte] = v.asInstanceOf[Array[Byte]]
  }

  implicit val StringJListMappung = new ValueMapping[jList[String]] {
    override def toAerospikeValue(t: jList[String]): Value = new ListValue(t)
    override def fromStoredObject(v: Object): jList[String] = v.asInstanceOf[jList[String]]
  }
}
