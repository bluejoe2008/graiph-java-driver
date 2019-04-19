package org.neo4j.driver.internal.value

import org.neo4j.blob.{BlobHolder, Blob}
import org.neo4j.blob.utils.BlobIO._
import org.neo4j.blob.utils.ReflectUtils._
import org.neo4j.blob.utils.{PackOutputInterface, BlobIO, PackInputInterface, ReflectUtils}
import org.neo4j.driver.Value
import org.neo4j.driver.internal.types.{TypeConstructor, TypeRepresentation}
import org.neo4j.driver.types.Type

/**
  * Created by bluejoe on 2019/4/18.
  */
object BoltClientBlobIO {

  def readBlob(unpacker: org.neo4j.driver.internal.packstream.PackStream.Unpacker): Value = {
    val in = unpacker._get("in").asInstanceOf[org.neo4j.driver.internal.packstream.PackInput];
    _readBlob(new PackInputInterface() {
      def peekByte(): Byte = in.peekByte();

      def readByte(): Byte = in.readByte();

      def readBytes(bytes: Array[Byte], offset: Int, toRead: Int) = in.readBytes(bytes, offset, toRead);

      def readInt(): Int = in.readInt();

      def readLong(): Long = in.readLong();
    }).map(new RemoteBlobValue(_)).getOrElse(null);
  }

  //client side?
  def writeBlob(blob: Blob, packer: org.neo4j.driver.internal.packstream.PackStream.Packer): Unit = {
    val out = packer._get("out").asInstanceOf[org.neo4j.driver.internal.packstream.PackOutput];
    val out2 =
      new PackOutputInterface() {
        override def writeByte(b: Byte): Unit = out.writeByte(b);

        override def writeInt(i: Int): Unit = out.writeInt(i);

        override def writeBytes(bs: Array[Byte]): Unit = out.writeBytes(bs);

        override def writeLong(l: Long): Unit = out.writeLong(l);
      }

    _writeBlob(blob, out2);
  }
}

class RemoteBlobValue(val blob: Blob)
  extends ValueAdapter with BlobHolder {

  val BOLT_BLOB_TYPE = new TypeRepresentation(TypeConstructor.BLOB);

  override def `type`(): Type = BOLT_BLOB_TYPE;

  override def equals(obj: Any): Boolean = obj.isInstanceOf[RemoteBlobValue] &&
    obj.asInstanceOf[RemoteBlobValue].blob.equals(this.blob);

  override def hashCode: Int = blob.hashCode()

  override def asBlob: Blob = blob;

  override def asObject = blob;

  override def toString: String = s"BoltBlobValue(blob=${blob.toString})"
}