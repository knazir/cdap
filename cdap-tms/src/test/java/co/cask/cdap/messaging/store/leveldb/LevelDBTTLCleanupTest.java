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

package co.cask.cdap.messaging.store.leveldb;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.messaging.store.TTLCleanupTest;
import co.cask.cdap.messaging.store.TableFactory;
import co.cask.cdap.proto.id.NamespaceId;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Tests for TTL Cleanup logic in LevelDB.
 */
// Tests ignored since they are failing now. Yet to debug.
@Ignore
public class LevelDBTTLCleanupTest extends TTLCleanupTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static TableFactory tableFactory;

  @BeforeClass
  public static void init() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.MessagingSystem.LOCAL_TTL_CLEANUP_FREQUENCY, Long.toString(1));
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    tableFactory = new LevelDBTableFactory(cConf);
  }

  @Override
  protected void forceFlushAndCompact(Table table) throws Exception {
    // since we have a periodic thread doing the clean up, we don't/can't do much here.
  }

  @Override
  protected MetadataTable getMetadataTable() throws Exception {
    return tableFactory.createMetadataTable(NamespaceId.CDAP, "metadata");
  }

  @Override
  protected PayloadTable getPayloadTable() throws Exception {
    return tableFactory.createPayloadTable(NamespaceId.CDAP, "payload");
  }

  @Override
  protected MessageTable getMessageTable() throws Exception {
    return tableFactory.createMessageTable(NamespaceId.CDAP, "message");
  }
}
