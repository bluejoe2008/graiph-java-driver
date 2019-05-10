package org.neo4j.driver.internal.value

import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.util.concurrent.CompletableFuture

import org.neo4j.blob._
import org.neo4j.blob.utils.Logging
import org.neo4j.driver.internal._
import org.neo4j.driver.internal.spi.Connection
import org.neo4j.driver.internal.types.{TypeConstructor, TypeRepresentation}
import org.neo4j.driver.types.Type

/**
  * Created by bluejoe on 2019/5/3.
  */
class InternalBlobValue(val blob: Blob)
  extends ValueAdapter with BlobHolder {

  val BOLT_BLOB_TYPE = new TypeRepresentation(TypeConstructor.BLOB);

  override def `type`(): Type = BOLT_BLOB_TYPE;

  override def equals(obj: Any): Boolean = obj.isInstanceOf[InternalBlobValue] &&
    obj.asInstanceOf[InternalBlobValue].blob.equals(this.blob);

  override def hashCode: Int = blob.hashCode()

  override def asBlob: Blob = blob;

  override def asObject = blob;

  override def toString: String = s"BoltBlobValue(blob=${blob.toString})"
}

case class BlobChunk(currentIndex: Int, bytes: Array[Byte], length: Int, total: Int) {
}

class RemoteBlob(conn: Connection, remoteHandle: String, val length: Long, val mimeType: MimeType)
  extends Blob with Logging {

  val FETCH_CHUNK_SIZE = 1024 * 10;
  //10k

  override val streamSource: InputStreamSource = new InputStreamSource() {
    def offerStream[T](consume: (InputStream) => T): T = {
      val is: InputStream =
        if (length == 0) {
          new ByteArrayInputStream(Array[Byte]());
        }
        else {
          val error = new CompletableFuture[Throwable]();
          val report = new CompletableFuture[Array[CompletableFuture[BlobChunk]]]();
          val handler = new GetBlobMessageHandler(report, error);
          //conn.cloneConnection().writeAndFlush(new GetBlobMessage(remoteHandle), handler);
          conn.writeAndFlush(new GetBlobMessage(remoteHandle), handler);
          val chunkFutures = report.get();
          new BlobInputStream(chunkFutures, error);
        }

      consume(is);
    }
  }
}

class BlobInputStream(chunkFutures: Array[CompletableFuture[BlobChunk]], error: CompletableFuture[Throwable]) extends InputStream with Logging {
  //maybe blob is validated
  checkErrors();

  val firstChunk = chunkFutures(0).get();
  var currentChunk: BlobChunk = firstChunk;
  var currentChunkInputStream = new ByteArrayInputStream(firstChunk.bytes);

  @throws[IOException]
  override def read(): Int = {
    val byte = currentChunkInputStream.read();
    if (byte != -1) {
      byte
    }
    //this chunk is consumed
    else {
      //end of file
      if (currentChunk.currentIndex == currentChunk.total - 1) {
        -1;
      }
      else {
        readNextChunk();
        read();
      }
    }
  }

  private def checkErrors(): Unit = {
    if (error.isDone && error.get() != null) {
      throw new FailedToReadStreamException(error.get);
    }
  }

  @throws[IOException]
  private def readNextChunk() = {
    val in = currentChunk.currentIndex;
    //TODO: discard
    currentChunk = chunkFutures(in + 1).get();
    checkErrors();
    currentChunkInputStream = new ByteArrayInputStream(currentChunk.bytes);
  }
}

class FailedToReadStreamException(cause: Throwable) extends RuntimeException(cause) {

}