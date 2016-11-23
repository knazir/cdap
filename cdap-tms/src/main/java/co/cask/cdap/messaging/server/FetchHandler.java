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

import co.cask.cdap.messaging.MessagingService;
import co.cask.http.AbstractHttpHandler;

import javax.ws.rs.Path;

/**
 * A netty http handler for handling message fetching REST API for the messaging system.
 */
@Path("/v1/namespaces/{namespace}")
public final class FetchHandler extends AbstractHttpHandler {

  private final MessagingService messagingService;

  FetchHandler(MessagingService messagingService) {
    this.messagingService = messagingService;
  }
}
