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

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.service.CoreMessagingService;
import co.cask.cdap.messaging.store.TableFactory;
import co.cask.cdap.messaging.store.leveldb.LevelDBTableFactory;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MessagingHttpServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = new Gson();
  private static final Type PROPERTIES_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type TOPICS_LIST_TYPE = new TypeToken<List<String>>() { } .getType();

  private static CConfiguration cConf;
  private static Injector injector;
  private static MessagingHttpService httpService;

  @BeforeClass
  public static void init() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.MessagingSystem.TOPIC_DEFAULT_TTL_SECONDS, "10");

    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new PrivateModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).toInstance(new NoOpMetricsCollectionService());
          bind(TableFactory.class).to(LevelDBTableFactory.class).in(Scopes.SINGLETON);
          bind(MessagingService.class).to(CoreMessagingService.class).in(Scopes.SINGLETON);

          bind(MessagingHttpService.class);
          expose(MessagingHttpService.class);
        }
      }
    );

    MessagingHttpServiceTest.cConf = injector.getInstance(CConfiguration.class);

    httpService = injector.getInstance(MessagingHttpService.class);
    httpService.startAndWait();
  }

  @AfterClass
  public static void finish() {
    httpService.stopAndWait();
  }

  @Test
  public void testMetadataEndpoints() throws Exception {
    // Get a non exist topic should return 404 Not Found
    HttpResponse response = HttpRequests.execute(HttpRequest.get(createURL("ns1/topics/t1")).build());
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, response.getResponseCode());

    // Create the topic t1
    response = HttpRequests.execute(HttpRequest.put(createURL("ns1/topics/t1")).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    // Create an existing topic should return 409 Conflict
    response = HttpRequests.execute(HttpRequest.put(createURL("ns1/topics/t1")).build());
    Assert.assertEquals(HttpURLConnection.HTTP_CONFLICT, response.getResponseCode());

    // Get the topic properties. Verify TTL is the same as the default one
    response = HttpRequests.execute(HttpRequest.get(createURL("ns1/topics/t1")).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Map<String, String> properties = GSON.fromJson(response.getResponseBodyAsString(), PROPERTIES_TYPE);
    Assert.assertEquals(cConf.get(Constants.MessagingSystem.TOPIC_DEFAULT_TTL_SECONDS), properties.get("ttl"));

    // Update the topic t1 with new TTL
    properties.put("ttl", "5");
    response = HttpRequests.execute(HttpRequest.put(createURL("ns1/topics/t1/properties"))
                                               .withBody(GSON.toJson(properties)).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    // Get the topic t1 properties. Verify TTL is updated
    response = HttpRequests.execute(HttpRequest.get(createURL("ns1/topics/t1")).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    properties = GSON.fromJson(response.getResponseBodyAsString(), PROPERTIES_TYPE);
    Assert.assertEquals("5", properties.get("ttl"));

    // Try to add another topic t2 with invalid ttl, it should fail with 400
    properties.put("ttl", "xyz");
    response = HttpRequests.execute(HttpRequest.put(createURL("ns1/topics/t2"))
                                               .withBody(GSON.toJson(properties)).build());
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.getResponseCode());

    // Add topic t2 with valid ttl
    properties.put("ttl", "5");
    response = HttpRequests.execute(HttpRequest.put(createURL("ns1/topics/t2"))
                                      .withBody(GSON.toJson(properties)).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    // Get the topic t2 properties. It should have TTL set based on what provided
    response = HttpRequests.execute(HttpRequest.get(createURL("ns1/topics/t1")).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    properties = GSON.fromJson(response.getResponseBodyAsString(), PROPERTIES_TYPE);
    Assert.assertEquals("5", properties.get("ttl"));

    // Listing topics under namespace ns1
    response = HttpRequests.execute(HttpRequest.get(createURL("ns1/topics")).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(Arrays.asList("t1", "t2"), GSON.fromJson(response.getResponseBodyAsString(), TOPICS_LIST_TYPE));

    // Delete both topics
    response = HttpRequests.execute(HttpRequest.delete(createURL("ns1/topics/t1")).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    response = HttpRequests.execute(HttpRequest.delete(createURL("ns1/topics/t2")).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    // Delete a non exist topic should get 404 Not Found
    response = HttpRequests.execute(HttpRequest.delete(createURL("ns1/topics/t1")).build());
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, response.getResponseCode());

    // Update properties on a non exist topic should get 404 Not Found
    response = HttpRequests.execute(HttpRequest.put(createURL("ns1/topics/t1/properties"))
                                      .withBody(GSON.toJson(properties)).build());
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, response.getResponseCode());

    // Listing topics under namespace ns1 again, it should be empty
    response = HttpRequests.execute(HttpRequest.get(createURL("ns1/topics")).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(Collections.emptyList(), GSON.fromJson(response.getResponseBodyAsString(), TOPICS_LIST_TYPE));
  }

  private URL createURL(String path) throws MalformedURLException {
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    Discoverable endpoint = new RandomEndpointStrategy(
      discoveryServiceClient.discover(Constants.Service.MESSAGING_SERVICE)).pick(10, TimeUnit.SECONDS);
    Assert.assertNotNull(endpoint);

    InetSocketAddress address = endpoint.getSocketAddress();
    URI baseURI = URI.create(String.format("http://%s:%d/v1/namespaces/", address.getHostName(), address.getPort()));
    return baseURI.resolve(path).toURL();
  }
}
