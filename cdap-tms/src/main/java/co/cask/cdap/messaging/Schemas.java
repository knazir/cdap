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

package co.cask.cdap.messaging;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import javax.annotation.Nullable;

/**
 * Helper class for getting various {@link Schema} used by the messaging system.
 */
public final class Schemas {

  private static final Logger LOG = LoggerFactory.getLogger(Schemas.class);

  /**
   * Contains Schema for V1 protocol.
   */
  public static final class V1 {

    public static final class PublishRequest {
      public static final Schema SCHEMA = loadSchema(PublishRequest.class);
    }

    public static final class PublishResponse {
      public static final Schema SCHEMA = loadSchema(PublishResponse.class);
    }

    public static final class ConsumeRequest {
      public static final Schema SCHEMA = loadSchema(ConsumeRequest.class);
    }

    public static final class ConsumeResponse {
      public static final Schema SCHEMA = loadSchema(ConsumeResponse.class);
    }
  }

  private Schemas() {
    // protect the constructor
  }

  @Nullable
  private static Schema loadSchema(Class<?> cls) {
    String resourceName = String.format("schema/%s/%s.schema",
                                        cls.getDeclaringClass().getSimpleName().toLowerCase(),
                                        cls.getSimpleName());
    URL resource = cls.getClassLoader().getResource(resourceName);
    if (resource == null) {
      LOG.warn("Failed to load schema from resource {}", resourceName);
      return null;
    }

    try (InputStream is = resource.openStream()) {
      return new Schema.Parser().parse(is);
    } catch (IOException e) {
      // Shouldn't happen
      LOG.warn("Failed to read schema from resource {}", resourceName, e);
      return null;
    }
  }

}
