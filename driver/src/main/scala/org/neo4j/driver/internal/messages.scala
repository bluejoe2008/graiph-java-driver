package org.neo4j.driver.internal

import java.util.concurrent.CompletableFuture

import org.neo4j.blob.BlobMessageSignature
import org.neo4j.blob.utils.Logging
import org.neo4j.driver.Value
import org.neo4j.driver.internal.messaging.{Message, MessageEncoder, ValuePacker}
import org.neo4j.driver.internal.spi.ResponseHandler
import org.neo4j.driver.internal.util.Preconditions._
import org.neo4j.driver.internal.value.BlobChunk

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2019/4/18.
  */

class GetBlobMessage(val blodId: String) extends Message {
  def signature: Byte = {
    return BlobMessageSignature.SIGNATURE_GET_BLOB;
  }

  override def toString: String = {
    return "GetBlob"
  }
}

class GetBlobMessageEncoder extends MessageEncoder {
  override def encode(message: Message, packer: ValuePacker): Unit = {
    checkArgument(message, classOf[GetBlobMessage])
    packer.packStructHeader(1, BlobMessageSignature.SIGNATURE_GET_BLOB)
    packer.pack((message.asInstanceOf[GetBlobMessage]).blodId)
  }
}

class GetBlobMessageHandler(report: CompletableFuture[Array[CompletableFuture[BlobChunk]]], exception: CompletableFuture[Throwable])
  extends ResponseHandler with Logging {
  var _chunks: Array[CompletableFuture[BlobChunk]] = null;

  override def onSuccess(metadata: java.util.Map[String, Value]): Unit = {
    exception.complete(null);
  }

  override def onRecord(fields: Array[Value]): Unit = {
    val chunk = new BlobChunk(fields(0).asInt(), fields(1).asByteArray(), fields(2).asInt(), fields(3).asInt());

    //first!
    if (chunk.currentIndex == 0) {
      val chunks = ArrayBuffer[CompletableFuture[BlobChunk]]();
      chunks ++= (0 to chunk.total - 1).map(x => new CompletableFuture[BlobChunk]());
      chunks(0).complete(chunk);

      _chunks = chunks.toArray;
      report.complete(_chunks);
    }
    else {
      _chunks(chunk.currentIndex).complete(chunk);
    }
  }

  override def onFailure(error: Throwable): Unit = {
    exception.complete(error);

    if (!report.isDone)
      report.complete(null);

    _chunks.foreach { x =>
      if (!x.isDone)
        x.complete(null)
    }
  }
}