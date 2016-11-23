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

package co.cask.cdap.internal.app.preview;

import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.preview.PreviewManager;
import co.cask.cdap.app.preview.PreviewModule;
import co.cask.cdap.app.preview.PreviewStatus;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.preview.PreviewDiscoveryRuntimeModule;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.config.guice.ConfigStoreModule;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.preview.PreviewDataModules;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.data2.transaction.stream.inmemory.InMemoryStreamConsumerFactory;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.client.MockExploreClient;
import co.cask.cdap.gateway.handlers.meta.RemoteSystemOperationsServiceModule;
import co.cask.cdap.internal.app.namespace.LocalStorageProviderNamespaceAdmin;
import co.cask.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactStore;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.guice.SecurityModules;
import co.cask.cdap.security.guice.preview.PreviewSecureStoreModule;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.gson.JsonElement;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Class responsible for creating the injector for preview and starting it.
 */
public class DelegatingPreviewManager implements PreviewManager {

  private static final Logger LOG = LoggerFactory.getLogger(DelegatingPreviewManager.class);
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final DiscoveryService discoveryService;
  private final DatasetFramework datasetFramework;
  private final PreferencesStore preferencesStore;
  private final SecureStore secureStore;
  private final TransactionManager transactionManager;
  private final ArtifactRepository artifactRepository;
  private final ArtifactStore artifactStore;
  private final AuthorizerInstantiator authorizerInstantiator;
  private final StreamAdmin streamAdmin;
  private final StreamCoordinatorClient streamCoordinatorClient;
  private final PrivilegesManager privilegesManager;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final Cache<ApplicationId, Injector> appInjectors;

  @Inject
  DelegatingPreviewManager(final CConfiguration cConf, Configuration hConf, DiscoveryService discoveryService,
                           @Named(DataSetsModules.BASE_DATASET_FRAMEWORK) DatasetFramework datasetFramework,
                           PreferencesStore preferencesStore, SecureStore secureStore,
                           TransactionManager transactionManager, ArtifactRepository artifactRepository,
                           ArtifactStore artifactStore, AuthorizerInstantiator authorizerInstantiator,
                           StreamAdmin streamAdmin, StreamCoordinatorClient streamCoordinatorClient,
                           PrivilegesManager privilegesManager, AuthorizationEnforcer authorizationEnforcer) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.datasetFramework = datasetFramework;
    this.discoveryService = discoveryService;
    this.preferencesStore = preferencesStore;
    this.secureStore = secureStore;
    this.transactionManager = transactionManager;
    this.artifactRepository = artifactRepository;
    this.artifactStore = artifactStore;
    this.authorizerInstantiator = authorizerInstantiator;
    this.streamAdmin = streamAdmin;
    this.streamCoordinatorClient = streamCoordinatorClient;
    this.privilegesManager = privilegesManager;
    this.authorizationEnforcer = authorizationEnforcer;

    // TODO make maximum size and expire after write configurable?
    this.appInjectors = CacheBuilder.newBuilder()
      .maximumSize(10)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .removalListener(new RemovalListener<ApplicationId, Injector>() {
        @Override
        @ParametersAreNonnullByDefault
        public void onRemoval(RemovalNotification<ApplicationId, Injector> notification) {
          Injector injector = notification.getValue();
          if (injector != null) {
            PreviewRuntimeService service = injector.getInstance(PreviewRuntimeService.class);
            service.stopAndWait();
          }
          ApplicationId application = notification.getKey();
          if (application == null) {
            return;
          }
          java.nio.file.Path previewDirPath = Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                        "preview", application.getApplication()).toAbsolutePath();

          try {
            DirUtils.deleteDirectoryContents(previewDirPath.toFile(), false);
          } catch (IOException e) {
            LOG.warn("Error deleting the preview directory {}", previewDirPath, e);
          }
        }
      })
      .build();
  }

  /**
   * Stop the preview run.
   * @param application the id of the preview application
   * @throws Exception when there is error during stop
   */
  public void stop(ApplicationId application) throws Exception {
    Injector injector = appInjectors.getIfPresent(application);
    if (injector == null) {
      throw new NotFoundException(application);
    }
    PreviewManager manager = injector.getInstance(PreviewManager.class);
    manager.stop(application);
  }

  @Override
  public List<String> getTracers(ApplicationId preview) throws NotFoundException {
    return null;
  }

  @Override
  public void start(ApplicationId previewApp, AppRequest<?> appRequest) throws Exception {
    Set<String> realDatasets = appRequest.getPreview() == null ? new HashSet<String>()
      : appRequest.getPreview().getRealDatasets();

    Injector injector = createPreviewInjector(previewApp, realDatasets);
    appInjectors.put(previewApp, injector);
    PreviewRuntimeService service = injector.getInstance(PreviewRuntimeService.class);
    service.startAndWait();

    PreviewManager manager = injector.getInstance(PreviewManager.class);
    manager.start(previewApp, appRequest);
  }

  /**
   * Get the status of the preview application.
   * @param application the id of the preview application
   * @return {@link PreviewStatus} of the preview application
   * @throws NotFoundException when preview application not found
   */
  public PreviewStatus getStatus(ApplicationId application) throws NotFoundException {
    Injector injector = appInjectors.getIfPresent(application);
    if (injector == null) {
      throw new NotFoundException(application);
    }

    PreviewManager manager = injector.getInstance(PreviewManager.class);
    return manager.getStatus(application);
  }

  /**
   * Get the data associated with the preview application for a given tracer.
   * @param application the id of the preview application
   * @param tracerId the id of the tracer for which data to be fetched
   * @return {@link Map} of properties associated with the tracer for a given preview application
   * @throws NotFoundException when preview application not found
   */
  public Map<String, List<JsonElement>> getData(ApplicationId application, String tracerId) throws NotFoundException {
    Injector injector = appInjectors.getIfPresent(application);
    if (injector == null) {
      throw new NotFoundException(application);
    }

    PreviewManager manager = injector.getInstance(PreviewManager.class);
    return manager.getData(application, tracerId);
  }

  @Override
  public Collection<MetricTimeSeries> getMetrics(ApplicationId preview) throws NotFoundException {
    return Collections.emptyList();
  }

  @Override
  public List<LogEntry> getLogs(ApplicationId preview) throws NotFoundException {
    return new ArrayList<>();
  }

  /**
   * Create injector for the given application id.
   */
  @VisibleForTesting
  Injector createPreviewInjector(ApplicationId applicationId, Set<String> datasetNames) throws IOException {
    CConfiguration previewcConf = CConfiguration.copy(cConf);
    java.nio.file.Path previewDirPath = Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR), "preview").toAbsolutePath();

    Files.createDirectories(previewDirPath);
    java.nio.file.Path previewDir = Files.createDirectory(Paths.get(previewDirPath.toAbsolutePath().toString(),
                                                                    applicationId.getApplication()));
    previewcConf.set(Constants.CFG_LOCAL_DATA_DIR, previewDir.toString());
    previewcConf.set(Constants.Dataset.DATA_DIR, previewDir.toString());
    Configuration previewhConf = new Configuration(hConf);
    previewhConf.set(Constants.CFG_LOCAL_DATA_DIR, previewDir.toString());
    previewcConf.setIfUnset(Constants.CFG_DATA_LEVELDB_DIR, previewDir.toString());
    previewcConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, false);

    return Guice.createInjector(
      new ConfigModule(previewcConf, previewhConf),
      new IOModule(),
      new AuthenticationContextModules().getMasterModule(),
      new SecurityModules().getStandaloneModules(),
      new PreviewSecureStoreModule(secureStore),
      new PreviewDiscoveryRuntimeModule(discoveryService),
      new LocationRuntimeModule().getStandaloneModules(),
      new ConfigStoreModule().getStandaloneModule(),
      new PreviewModule(),
      new ProgramRunnerRuntimeModule().getStandaloneModules(),
      new PreviewDataModules().getDataFabricModule(transactionManager),
      new PreviewDataModules().getDataSetsModule(datasetFramework, datasetNames),
      new DataSetServiceModules().getStandaloneModules(),
      new MetricsClientRuntimeModule().getStandaloneModules(),
      new LoggingModules().getStandaloneModules(),
      new NamespaceStoreModule().getStandaloneModules(),
      new RemoteSystemOperationsServiceModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(ArtifactRepository.class).toInstance(artifactRepository);
          bind(ArtifactStore.class).toInstance(artifactStore);
          bind(AuthorizerInstantiator.class).toInstance(authorizerInstantiator);
          bind(AuthorizationEnforcer.class).toInstance(authorizationEnforcer);
          bind(PrivilegesManager.class).toInstance(privilegesManager);
          bind(StreamAdmin.class).toInstance(streamAdmin);
          bind(StreamConsumerFactory.class).to(InMemoryStreamConsumerFactory.class).in(Singleton.class);
          bind(StreamCoordinatorClient.class).toInstance(streamCoordinatorClient);
          bind(PreferencesStore.class).toInstance(preferencesStore);
          // bind explore client to mock.
          bind(ExploreClient.class).to(MockExploreClient.class);
          bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);
        }

        @Provides
        @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS)
        @SuppressWarnings("unused")
        public InetAddress providesHostname(CConfiguration cConf) {
          String address = cConf.get(Constants.Preview.ADDRESS);
          return Networks.resolve(address, new InetSocketAddress("localhost", 0).getAddress());
        }
      }
    );
  }
}
