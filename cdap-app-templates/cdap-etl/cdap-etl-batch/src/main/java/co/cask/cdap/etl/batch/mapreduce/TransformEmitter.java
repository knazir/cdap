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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.batch.BatchEmitter;
import co.cask.cdap.etl.batch.BatchTransformDetail;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public class TransformEmitter extends BatchEmitter<BatchTransformDetail> {
  private static final Logger LOG = LoggerFactory.getLogger(TransformEmitter.class);

  private final String stageName;
  private final Map<String, BatchTransformDetail> nextStages;
  @Nullable
  private final ErrorOutputWriter<Object, Object> errorOutputWriter;

  @VisibleForTesting
  public TransformEmitter(String stageName, Map<String, BatchTransformDetail> nextStages) {
    this.stageName = stageName;
    this.nextStages = nextStages;
    this.errorOutputWriter = null;
  }

  public TransformEmitter(String stageName, Map<String, BatchTransformDetail> nextStages,
                          ErrorOutputWriter<Object, Object> errorOutputWriter) {
    this.stageName = stageName;
    this.nextStages = nextStages;
    this.errorOutputWriter = errorOutputWriter;
  }

  @Override
  public void emit(Object value) {
    for (BatchTransformDetail etlTransformDetail : nextStages.values()) {
      LOG.info("Emitting from stage: {}", stageName);
      etlTransformDetail.process(value);
    }
  }

  @Override
  public void emitError(InvalidEntry<Object> invalidEntry) {
    try {
      if (errorOutputWriter == null) {
        LOG.warn("Transform : {} has error records, but does not have a error dataset configured.", stageName);
      } else {
        errorOutputWriter.write(invalidEntry);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void addTransformDetail(String stageName, BatchTransformDetail etlTransformDetail) {
    nextStages.put(stageName, etlTransformDetail);
  }

  public Map<String, BatchTransformDetail> getNextStages() {
    return nextStages;
  }
}
