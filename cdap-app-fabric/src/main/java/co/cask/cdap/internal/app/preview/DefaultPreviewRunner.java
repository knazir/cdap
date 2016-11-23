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

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.app.preview.PreviewRequest;
import co.cask.cdap.app.preview.PreviewRunner;
import co.cask.cdap.app.preview.PreviewStatus;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.store.preview.PreviewStore;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.inject.Inject;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.common.Threads;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of the {@link PreviewRunner}.
 */
public class DefaultPreviewRunner implements PreviewRunner {
  private static final Gson GSON = new Gson();
  private static final ProgramTerminator NOOP_PROGRAM_TERMINATOR = new ProgramTerminator() {
    @Override
    public void stop(ProgramId programId) throws Exception {
      // no-op
    }
  };
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ProgramLifecycleService programLifecycleService;
  private final PreviewStore previewStore;
  private PreviewStatus status;
  private ProgramId programId;

  @Inject
  DefaultPreviewRunner(ApplicationLifecycleService applicationLifecycleService,
                       ProgramLifecycleService programLifecycleService, PreviewStore previewStore) {
    this.applicationLifecycleService = applicationLifecycleService;
    this.programLifecycleService = programLifecycleService;
    this.previewStore = previewStore;
    this.status = null;
  }

  @Override
  public void start(PreviewRequest<?> previewRequest) throws Exception {
    programId = previewRequest.getProgram();
    AppRequest<?> request = previewRequest.getAppRequest();
    ArtifactSummary artifactSummary = request.getArtifact();
    ApplicationId preview = programId.getParent();
    NamespaceId artifactNamespace = ArtifactScope.SYSTEM.equals((artifactSummary.getScope())) ? NamespaceId.SYSTEM
      : preview.getParent();

    ArtifactId artifactId = new ArtifactId(artifactNamespace.getNamespace(), artifactSummary.getName(),
                                           artifactSummary.getVersion());

    String config = request.getConfig() == null ? null : GSON.toJson(request.getConfig());

    try {
      applicationLifecycleService.deployApp(preview.getParent(), preview.getApplication(), preview.getVersion(),
                                            artifactId.toId(), config, NOOP_PROGRAM_TERMINATOR);
    } catch (Exception e) {
      this.status = new PreviewStatus(PreviewStatus.Status.DEPLOY_FAILED, new BasicThrowable(e));
      throw e;
    }

    ProgramController controller = programLifecycleService.start(programId, new HashMap<String, String>(), false);

    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        status = new PreviewStatus(PreviewStatus.Status.RUNNING, null);
      }

      @Override
      public void completed() {
        setStatus(new PreviewStatus(PreviewStatus.Status.COMPLETED, null));
      }

      @Override
      public void killed() {
        setStatus(new PreviewStatus(PreviewStatus.Status.KILLED, null));
      }

      @Override
      public void error(Throwable cause) {
        setStatus(new PreviewStatus(PreviewStatus.Status.RUN_FAILED, new BasicThrowable(cause)));
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private synchronized void setStatus(PreviewStatus status) {
    this.status = status;
  }

  @Override
  public PreviewStatus getStatus() {
    return status;
  }

  @Override
  public void stop() throws Exception {
    programLifecycleService.stop(programId);
  }

  @Override
  public List<String> getTracers() {
    return new ArrayList<>();
  }

  @Override
  public Map<String, List<JsonElement>> getData(String tracerName) {
    return previewStore.get(programId.getParent(), tracerName);
  }

  @Override
  public Collection<MetricTimeSeries> getMetrics() {
    return Collections.emptyList();
  }

  @Override
  public List<LogEntry> getLogs() {
    return new ArrayList<>();
  }
}
