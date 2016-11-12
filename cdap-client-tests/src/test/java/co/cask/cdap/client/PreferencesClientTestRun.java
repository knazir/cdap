/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.client.app.AppReturnsArgs;
import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.XSlowTests;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link PreferencesClient}
 */
@Category(XSlowTests.class)
public class PreferencesClientTestRun extends ClientTestBase {

  private static final Gson GSON = new Gson();
  private static final Id.Application FAKE_APP_ID = Id.Application.from(Id.Namespace.DEFAULT, FakeApp.NAME);
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private PreferencesClient client;
  private ApplicationClient appClient;
  private ServiceClient serviceClient;
  private ProgramClient programClient;
  private NamespaceClient namespaceClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    client = new PreferencesClient(clientConfig);
    appClient = new ApplicationClient(clientConfig);
    serviceClient = new ServiceClient(clientConfig);
    programClient = new ProgramClient(clientConfig);
    namespaceClient = new NamespaceClient(clientConfig);
  }

  @Test
  public void testProgramAPI() throws Exception {
    Map<String, String> propMap = Maps.newHashMap();
    propMap.put("key", "instance");
    File jarFile = createAppJarFile(AppReturnsArgs.class);
    appClient.deploy(Id.Namespace.DEFAULT, jarFile);
    Id.Application app = Id.Application.from(Id.Namespace.DEFAULT, AppReturnsArgs.NAME);
    Id.Service service = Id.Service.from(app, AppReturnsArgs.SERVICE);

    try {
      client.setInstancePreferences(propMap);
      Map<String, String> setMap = Maps.newHashMap();
      setMap.put("saved", "args");

      programClient.setRuntimeArgs(service, setMap);
      assertEquals(setMap, programClient.getRuntimeArgs(service));

      propMap.put("run", "value");
      propMap.put("logical.start.time", "1234567890000");
      propMap.putAll(setMap);

      programClient.start(service, false, propMap);
      assertProgramRunning(programClient, service);

      URL serviceURL = new URL(serviceClient.getServiceURL(service), AppReturnsArgs.ENDPOINT);
      HttpResponse response = getServiceResponse(serviceURL);
      assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
      Map<String, String> responseMap = GSON.fromJson(response.getResponseBodyAsString(), STRING_MAP_TYPE);
      assertEquals(propMap, responseMap);
      programClient.stop(service);
      assertProgramStopped(programClient, service);

      long minStartTime = System.currentTimeMillis();
      client.deleteInstancePreferences();
      programClient.start(service);
      assertProgramRunning(programClient, service);
      propMap.remove("key");
      propMap.remove("run");
      propMap.remove("logical.start.time");

      serviceURL = new URL(serviceClient.getServiceURL(service), AppReturnsArgs.ENDPOINT);
      response = getServiceResponse(serviceURL);
      responseMap = GSON.fromJson(response.getResponseBodyAsString(), STRING_MAP_TYPE);
      long actualStartTime = Long.parseLong(responseMap.remove("logical.start.time"));
      Assert.assertTrue(actualStartTime >= minStartTime);
      assertEquals(propMap, responseMap);
      programClient.stop(service);
      assertProgramStopped(programClient, service);

      propMap.clear();
      minStartTime = System.currentTimeMillis();
      programClient.setRuntimeArgs(service, propMap);
      programClient.start(service);
      assertProgramRunning(programClient, service);
      serviceURL = new URL(serviceClient.getServiceURL(service), AppReturnsArgs.ENDPOINT);
      response = getServiceResponse(serviceURL);
      assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
      responseMap = GSON.fromJson(response.getResponseBodyAsString(), STRING_MAP_TYPE);
      actualStartTime = Long.parseLong(responseMap.remove("logical.start.time"));
      Assert.assertTrue(actualStartTime >= minStartTime);
      assertEquals(propMap, responseMap);
    } finally {
      programClient.stop(service);
      assertProgramStopped(programClient, service);
      appClient.delete(app);
    }
  }

  private HttpResponse getServiceResponse(URL serviceURL) throws IOException, InterruptedException {
    int iterations = 0;
    HttpResponse response;
    do {
      response = HttpRequests.execute(HttpRequest.builder(HttpMethod.GET, serviceURL).build());
      if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
        return response;
      }
      TimeUnit.MILLISECONDS.sleep(50);
      iterations++;
    } while (iterations <= 100);
    return response;
  }

  @Test
  public void testPreferences() throws Exception {
    Id.Namespace invalidNamespace = Id.Namespace.from("invalid");
    namespaceClient.create(new NamespaceMeta.Builder().setName(invalidNamespace.getId()).build());

    Map<String, String> propMap = client.getInstancePreferences();
    Assert.assertEquals(ImmutableMap.<String, String>of(), propMap);
    propMap.put("k1", "instance");
    client.setInstancePreferences(propMap);
    Assert.assertEquals(propMap, client.getInstancePreferences());

    File jarFile = createAppJarFile(FakeApp.class);
    appClient.deploy(Id.Namespace.DEFAULT, jarFile);

    try {
      propMap.put("k1", "namespace");
      client.setNamespacePreferences(Id.Namespace.DEFAULT, propMap);
      Assert.assertEquals(propMap, client.getNamespacePreferences(Id.Namespace.DEFAULT, true));
      Assert.assertEquals(propMap, client.getNamespacePreferences(Id.Namespace.DEFAULT, false));
      Assert.assertTrue(client.getNamespacePreferences(invalidNamespace, false).isEmpty());
      Assert.assertEquals("instance", client.getNamespacePreferences(invalidNamespace, true).get("k1"));

      client.deleteNamespacePreferences(Id.Namespace.DEFAULT);
      propMap.put("k1", "instance");
      Assert.assertEquals(propMap, client.getNamespacePreferences(Id.Namespace.DEFAULT, true));
      Assert.assertEquals(ImmutableMap.<String, String>of(),
                          client.getNamespacePreferences(Id.Namespace.DEFAULT, false));

      propMap.put("k1", "namespace");
      client.setNamespacePreferences(Id.Namespace.DEFAULT, propMap);
      Assert.assertEquals(propMap, client.getNamespacePreferences(Id.Namespace.DEFAULT, true));
      Assert.assertEquals(propMap, client.getNamespacePreferences(Id.Namespace.DEFAULT, false));

      propMap.put("k1", "application");
      client.setApplicationPreferences(FAKE_APP_ID, propMap);
      Assert.assertEquals(propMap, client.getApplicationPreferences(FAKE_APP_ID, true));
      Assert.assertEquals(propMap, client.getApplicationPreferences(FAKE_APP_ID, false));

      propMap.put("k1", "program");
      Id.Program flow = Id.Program.from(FAKE_APP_ID, ProgramType.FLOW, FakeApp.FLOWS.get(0));
      client.setProgramPreferences(flow, propMap);
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, true));
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, false));
      client.deleteProgramPreferences(flow);

      propMap.put("k1", "application");
      Assert.assertTrue(client.getProgramPreferences(flow, false).isEmpty());
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, true));

      client.deleteApplicationPreferences(FAKE_APP_ID);

      propMap.put("k1", "namespace");
      Assert.assertTrue(client.getApplicationPreferences(FAKE_APP_ID, false).isEmpty());
      Assert.assertEquals(propMap, client.getApplicationPreferences(FAKE_APP_ID, true));
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, true));

      client.deleteNamespacePreferences(Id.Namespace.DEFAULT);
      propMap.put("k1", "instance");
      Assert.assertTrue(client.getNamespacePreferences(Id.Namespace.DEFAULT, false).isEmpty());
      Assert.assertEquals(propMap, client.getNamespacePreferences(Id.Namespace.DEFAULT, true));
      Assert.assertEquals(propMap, client.getApplicationPreferences(FAKE_APP_ID, true));
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, true));

      client.deleteInstancePreferences();
      propMap.clear();
      Assert.assertEquals(propMap, client.getInstancePreferences());
      Assert.assertEquals(propMap, client.getNamespacePreferences(Id.Namespace.DEFAULT, true));
      Assert.assertEquals(propMap, client.getNamespacePreferences(Id.Namespace.DEFAULT, true));
      Assert.assertEquals(propMap, client.getApplicationPreferences(FAKE_APP_ID, true));
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, true));


      //Test Deleting Application
      propMap.put("k1", "application");
      client.setApplicationPreferences(FAKE_APP_ID, propMap);
      Assert.assertEquals(propMap, client.getApplicationPreferences(FAKE_APP_ID, false));

      propMap.put("k1", "program");
      client.setProgramPreferences(flow, propMap);
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, false));

      appClient.delete(FAKE_APP_ID);
      // deleting the app should have deleted the preferences that were stored. so deploy the app and check
      // if the preferences are empty. we need to deploy the app again since getting preferences of non-existent apps
      // is not allowed by the API.
      appClient.deploy(Id.Namespace.DEFAULT, jarFile);
      propMap.clear();
      Assert.assertEquals(propMap, client.getApplicationPreferences(FAKE_APP_ID, false));
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, false));
    } finally {
      try {
        appClient.delete(FAKE_APP_ID);
      } catch (ApplicationNotFoundException e) {
        // ok if this happens, means its already deleted.
      }
      namespaceClient.delete(invalidNamespace.toEntityId());
    }
  }

  @Test
  public void testDeletingNamespace() throws Exception {
    Map<String, String> propMap = Maps.newHashMap();
    propMap.put("k1", "namespace");

    NamespaceId myspace = new NamespaceId("myspace");
    namespaceClient.create(new NamespaceMeta.Builder().setName(myspace).build());

    client.setNamespacePreferences(myspace.toId(), propMap);
    Assert.assertEquals(propMap, client.getNamespacePreferences(myspace.toId(), false));
    Assert.assertEquals(propMap, client.getNamespacePreferences(myspace.toId(), true));

    namespaceClient.delete(myspace);
    namespaceClient.create(new NamespaceMeta.Builder().setName(myspace).build());
    Assert.assertTrue(client.getNamespacePreferences(myspace.toId(), false).isEmpty());
    Assert.assertTrue(client.getNamespacePreferences(myspace.toId(), true).isEmpty());

    namespaceClient.delete(myspace);
  }

  @Test(expected = NotFoundException.class)
  public void testInvalidNamespace() throws Exception {
    Id.Namespace somespace = Id.Namespace.from("somespace");
    client.setNamespacePreferences(somespace, ImmutableMap.of("k1", "v1"));
  }

  @Test(expected = NotFoundException.class)
  public void testInvalidApplication() throws Exception {
    Id.Application someapp = Id.Application.from("somespace", "someapp");
    client.getApplicationPreferences(someapp, true);
  }

  @Test(expected = ProgramNotFoundException.class)
  public void testInvalidProgram() throws Exception {
    Id.Application someapp = Id.Application.from("somespace", "someapp");
    client.deleteProgramPreferences(Id.Program.from(someapp, ProgramType.FLOW, "myflow"));
  }
}
