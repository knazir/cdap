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

package co.cask.cdap.etl.api.batch;

import co.cask.cdap.etl.api.Emitter;

import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 * @param <T>
 */
public abstract class BatchEmitter<T> implements Emitter<Object> {

  public abstract void addTransformDetail(String stageName, T etlTransformDetail);

  @Nullable
  public abstract Map<String, T> getNextStages();
}
