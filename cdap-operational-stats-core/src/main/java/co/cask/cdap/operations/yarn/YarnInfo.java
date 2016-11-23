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

package co.cask.cdap.operations.yarn;

import co.cask.cdap.operations.OperationalStats;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.yarn.client.RMHAServiceTarget;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;

/**
 * {@link OperationalStats} for collecting YARN info.
 */
public class YarnInfo extends AbstractYarnStats implements YarnInfoMXBean {

  private String webUrl;
  private String logsUrl;

  @SuppressWarnings("unused")
  public YarnInfo() {
    this(new Configuration());
  }

  @VisibleForTesting
  YarnInfo(Configuration conf) {
    super(conf);
  }

  @Override
  public String getStatType() {
    return "info";
  }

  @Override
  public String getVersion() {
    return YarnVersionInfo.getVersion();
  }

  @Override
  public String getWebURL() {
    return webUrl;
  }

  @Override
  public String getLogsURL() {
    return logsUrl;
  }

  @Override
  public synchronized void collect() throws IOException {
    webUrl = getResourceManager().toString();
    logsUrl = webUrl + "/logs";
  }

  private URL getResourceManager() throws IOException {
    if (HAUtil.isHAEnabled(conf)) {
      return getHAWebURL();
    }
    return getNonHAWebURL();
  }

  private URL getNonHAWebURL() throws MalformedURLException {
    String protocol;
    String hostPort;
    if (isHttpsEnabled()) {
      protocol = "https";
      hostPort = conf.get(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS);
    } else {
      protocol = "http";
      hostPort = conf.get(YarnConfiguration.RM_WEBAPP_ADDRESS);
    }
    int portBeginIndex = hostPort.indexOf(":") + 1;
    String host = hostPort.substring(0, portBeginIndex - 1);
    int port = Integer.parseInt(hostPort.substring(portBeginIndex));
    return new URL(protocol, host, port, "");
  }

  /**
   * Should only be called when HA is enabled.
   */
  private URL getHAWebURL() throws IOException {
    InetSocketAddress activeRM = null;
    Collection<String> rmIds = HAUtil.getRMHAIds(conf);
    if (rmIds.isEmpty()) {
      throw new IllegalStateException("");
    }
    for (String rmId : rmIds) {
      YarnConfiguration yarnConf = new YarnConfiguration(conf);
      yarnConf.set(YarnConfiguration.RM_HA_ID, rmId);
      RMHAServiceTarget rmhaServiceTarget = new RMHAServiceTarget(yarnConf);
      HAServiceProtocol proxy = rmhaServiceTarget.getProxy(yarnConf, 10000);
      HAServiceStatus serviceStatus = proxy.getServiceStatus();
      if (HAServiceProtocol.HAServiceState.ACTIVE != serviceStatus.getState()) {
        continue;
      }
      activeRM = rmhaServiceTarget.getAddress();
    }
    if (activeRM == null) {
      throw new IllegalStateException("Could not find an active resource manager");
    }
    return adminToWebappAddress(activeRM);
  }

  private URL adminToWebappAddress(InetSocketAddress adminAddress) throws MalformedURLException {
    String host = adminAddress.getHostName();
    URL nonHAWebUrl = getNonHAWebURL();
    return new URL(nonHAWebUrl.getProtocol(), host, nonHAWebUrl.getPort(), "");
  }

  private boolean isHttpsEnabled() {
    String httpPolicy = conf.get(YarnConfiguration.YARN_HTTP_POLICY_KEY, YarnConfiguration.YARN_HTTP_POLICY_DEFAULT);
    return "HTTPS_ONLY".equals(httpPolicy);
  }
}
