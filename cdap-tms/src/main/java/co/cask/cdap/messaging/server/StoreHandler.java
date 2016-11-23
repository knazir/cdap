/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.messaging.server;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.messaging.MessageRollback;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.Schemas;
import co.cask.cdap.messaging.service.StoreRequest;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A netty http handler for handling message storage REST API for the messaging system.
 */
@Path("/v1/namespaces/{namespace}")
public final class StoreHandler extends AbstractHttpHandler {

  private static final ThreadLocal<BinaryEncoder> ENCODER_THREAD_LOCAL = new ThreadLocal<>();
  private static final ThreadLocal<BinaryDecoder> DECODER_THREAD_LOCAL = new ThreadLocal<>();
  private static final ThreadLocal<DatumReader<?>> DATUM_READER_THREAD_LOCAL = new ThreadLocal<DatumReader<?>>() {
    @Override
    protected DatumReader<?> initialValue() {
      return new GenericDatumReader<>();
    }
  };
  private static final ThreadLocal<GenericRecord> PUBLISH_RECORD_THREAD_LOCAL = new ThreadLocal<>();

  private final MessagingService messagingService;

  StoreHandler(MessagingService messagingService) {
    this.messagingService = messagingService;
  }

  @POST
  @Path("/topics/{topic}/publish")
  public void publish(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace") String namespace,
                      @PathParam("topic") String topic) throws Exception {

    TopicId topicId = new NamespaceId(namespace).topic(topic);
    StoreRequest storeRequest = createStoreRequest(topicId, request);

    // Empty payload is only allowed for transactional publish
    if (!storeRequest.isTransactional() && !storeRequest.hasNext()) {
      throw new BadRequestException("Empty payload is only allowed for publishing transactional message. Topic: "
                                      + topicId);
    }

    MessageRollback messageRollback = messagingService.publish(storeRequest);


    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/topics/{topic}/store")
  public void store(HttpRequest request, HttpResponder responder,
                    @PathParam("namespace") String namespace,
                    @PathParam("topic") String topic) throws Exception {

    TopicId topicId = new NamespaceId(namespace).topic(topic);
    StoreRequest storeRequest = createStoreRequest(topicId, request);

    // It must be transactional with payload for store request
    if (!storeRequest.isTransactional() || !storeRequest.hasNext()) {
      throw new BadRequestException("Store request must be transactional with payload. Topic: " + topicId);
    }

    messagingService.storePayload(storeRequest);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private StoreRequest createStoreRequest(TopicId topicId, HttpRequest request) throws Exception {
    // Currently only support avro
    if (!"avro/binary".equals(request.getHeader(HttpHeaders.Names.CONTENT_TYPE))) {
      throw new BadRequestException("Only avro/binary content type is support.");
    }

    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(new ChannelBufferInputStream(request.getContent()),
                                                                     DECODER_THREAD_LOCAL.get());
    DECODER_THREAD_LOCAL.set(decoder);

    @SuppressWarnings("unchecked")
    DatumReader<GenericRecord> datumReader = (DatumReader<GenericRecord>) DATUM_READER_THREAD_LOCAL.get();
    datumReader.setSchema(Schemas.V1.PublishRequest.SCHEMA);

    GenericRecord genericRecord = datumReader.read(PUBLISH_RECORD_THREAD_LOCAL.get(), decoder);
    PUBLISH_RECORD_THREAD_LOCAL.set(genericRecord);

    return new GenericRecordStoreRequest(topicId, genericRecord);
  }

  private ChannelBuffer createPublishResponse(MessageRollback messageRollback) {
    // For V1 PublishResponse, it contains an union(long, null) and then 2 longs and 2 integers,
    // hence the max size is 38
    // (union use 1 byte, long max size is 9 bytes, integer max size is 5 bytes in avro binary encoding)
    ChannelBuffer buffer = ChannelBuffers.dynamicBuffer(38);
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(new ChannelBufferOutputStream(buffer), ENCODER_THREAD_LOCAL.get());

    return buffer;
  }

  /**
   * A {@link StoreRequest} that gets the request information from {@link GenericRecord}.
   */
  private static final class GenericRecordStoreRequest extends StoreRequest {

    private final Iterator<ByteBuffer> payloadIterator;

    @SuppressWarnings("unchecked")
    GenericRecordStoreRequest(TopicId topicId, GenericRecord genericRecord) {
      super(topicId, genericRecord.get("transactionWritePointer") != null,
            Long.parseLong(genericRecord.get("transactionWritePointer").toString()));

      this.payloadIterator = ((List<ByteBuffer>) genericRecord.get("messages")).iterator();
    }

    @Nullable
    @Override
    protected byte[] doComputeNext() {
      if (!payloadIterator.hasNext()) {
        return null;
      }
      ByteBuffer buffer = payloadIterator.next();
      if (buffer.hasArray() && buffer.array().length == buffer.remaining()) {
        return buffer.array();
      }
      return Bytes.toBytes(buffer);
    }
  }
}
