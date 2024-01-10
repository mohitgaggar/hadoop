package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

import java.util.LinkedHashMap;

public class ContainerQueueManager {
  private final LinkedHashMap<ContainerId, Container>
      queuedGuaranteedContainers = new LinkedHashMap<>();
  // Queue of Opportunistic Containers waiting for resources to run
  private final LinkedHashMap<ContainerId, Container>
      queuedOpportunisticContainers = new LinkedHashMap<>();

  public LinkedHashMap<ContainerId, Container> getQueuedGuaranteedContainers() {
    return queuedGuaranteedContainers;
  }

  public LinkedHashMap<ContainerId, Container> getQueuedOpportunisticContainers() {
    return queuedOpportunisticContainers;
  }
}
