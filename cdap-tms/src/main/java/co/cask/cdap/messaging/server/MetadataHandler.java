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

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A netty http handler for metadata REST API for the messaging system.
 */
@Path("/v1/namespaces/{namespace}")
public final class MetadataHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();
  private static final Type TOPIC_PROPERTY_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type TOPIC_LIST_TYPE = new TypeToken<List<String>>() { }.getType();
  private static final Function<TopicId, String> TOPIC_TO_NAME = new Function<TopicId, String>() {
    @Override
    public String apply(TopicId topicId) {
      return topicId.getTopic();
    }
  };

  private final CConfiguration cConf;
  private final MessagingService messagingService;

  MetadataHandler(CConfiguration cConf, MessagingService messagingService) {
    this.cConf = cConf;
    this.messagingService = messagingService;
  }

  @PUT
  @Path("/topics/{topic}")
  public void createTopic(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace") String namespace,
                          @PathParam("topic") String topic) throws Exception {
    TopicId topicId = new NamespaceId(namespace).topic(topic);
    Map<String, String> topicProperties = decodeTopicProperties(request.getContent(), createDefaultProperties());
    messagingService.createTopic(new TopicMetadata(topicId, topicProperties));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @PUT
  @Path("/topics/{topic}/properties")
  public void updateTopic(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace") String namespace,
                          @PathParam("topic") String topic) throws Exception {
    TopicId topicId = new NamespaceId(namespace).topic(topic);
    Map<String, String> topicProperties = decodeTopicProperties(request.getContent(), createDefaultProperties());
    messagingService.updateTopic(new TopicMetadata(topicId, topicProperties));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/topics/{topic}")
  public void getTopic(HttpRequest request, HttpResponder responder,
                       @PathParam("namespace") String namespace,
                       @PathParam("topic") String topic) throws Exception {
    TopicId topicId = new NamespaceId(namespace).topic(topic);
    TopicMetadata metadata = messagingService.getTopic(topicId);
    responder.sendJson(HttpResponseStatus.OK, metadata.getProperties(), TOPIC_PROPERTY_TYPE);
  }

  @GET
  @Path("/topics")
  public void listTopics(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace") String namespace) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       Lists.transform(messagingService.listTopics(new NamespaceId(namespace)), TOPIC_TO_NAME),
                       TOPIC_LIST_TYPE);
  }

  @DELETE
  @Path("/topics/{topic}")
  public void deleteTopic(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace") String namespace,
                          @PathParam("topic") String topic) throws Exception {
    TopicId topicId = new NamespaceId(namespace).topic(topic);
    messagingService.deleteTopic(topicId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private Map<String, String> createDefaultProperties() {
    Map<String, String> properties = new HashMap<>();

    // Default the TTL
    properties.put(TopicMetadata.TTL_KEY, cConf.get(Constants.MessagingSystem.TOPIC_DEFAULT_TTL_SECONDS));
    return properties;
  }

  private Map<String, String> decodeTopicProperties(ChannelBuffer channelBuffer,
                                                    Map<String, String> properties) throws BadRequestException {
    if (!channelBuffer.readable()) {
      return properties;
    }

    try {
      Map<String, String> decoded = GSON.fromJson(
        new InputStreamReader(new ChannelBufferInputStream(channelBuffer), StandardCharsets.UTF_8),
        TOPIC_PROPERTY_TYPE
      );
      properties.putAll(decoded);
      return properties;
    } catch (Exception e) {
      throw new BadRequestException("Invalid topic properties. It must be JSON object with string values.");
    }
  }
}
