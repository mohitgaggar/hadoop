package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;

public class ContainerStarter {

  private final LinkedHashMap<ContainerId, Container> runningContainers =
      new LinkedHashMap<>();

  private static Clock clock = SystemClock.getInstance();

  /**
   * Start pending containers in the queue.
   * @param forceStartGContainers When this is true, start guaranteed
   *        container without looking at available resource
   */
  public void startPendingContainers(boolean forceStartGContainers,
      ContainerQueueManager containerQueueManager,
      ResourceUtilizationTracker utilizationTracker,
      NodeManagerMetrics metrics) {
    // Start guaranteed containers that are paused, if resources available.
    boolean resourcesAvailable = startContainers(
        containerQueueManager.getQueuedGuaranteedContainers().values(),
        forceStartGContainers, utilizationTracker, metrics);
    // Start opportunistic containers, if resources available.
    if (resourcesAvailable) {
      startContainers(
          containerQueueManager.getQueuedOpportunisticContainers().values(),
          false, utilizationTracker, metrics);
    }
  }

  private boolean startContainers(Collection<Container> containersToBeStarted,
      boolean force, ResourceUtilizationTracker utilizationTracker,
      NodeManagerMetrics metrics) {
    Iterator<Container> cIter = containersToBeStarted.iterator();
    boolean resourcesAvailable = true;
    while (cIter.hasNext() && resourcesAvailable) {
      Container container = cIter.next();
      if (tryStartContainer(container, force, utilizationTracker, metrics)) {
        cIter.remove();
      } else {
        resourcesAvailable = false;
      }
    }
    return resourcesAvailable;
  }

  private boolean tryStartContainer(Container container, boolean force,
      ResourceUtilizationTracker utilizationTracker,
      NodeManagerMetrics metrics) {
    boolean containerStarted = false;
    // call startContainer without checking available resource when force==true
    if (force || resourceAvailableToStartContainer(container,
        utilizationTracker)) {
      startContainer(container, utilizationTracker, metrics);
      containerStarted = true;
    }
    return containerStarted;
  }

  /**
   * Check if there is resource available to start a given container
   * immediately. (This can be extended to include overallocated resources)
   *
   * @param container the container to start
   * @return true if container can be launched directly
   */
  private boolean resourceAvailableToStartContainer(Container container,
      ResourceUtilizationTracker utilizationTracker) {
    return utilizationTracker.hasResourcesAvailable(container);
  }

  private void startContainer(Container container,
      ResourceUtilizationTracker utilizationTracker,
      NodeManagerMetrics metrics) {
    LOG.info("Starting container [" + container.getContainerId() + "]");
    // Skip to put into runningContainers and addUtilization when recover
    if (!runningContainers.containsKey(container.getContainerId())) {
      runningContainers.put(container.getContainerId(), container);
      utilizationTracker.addContainerResources(container);
      //      logRunningContainers();
    }
    if (container.getContainerTokenIdentifier().getExecutionType()
        == ExecutionType.OPPORTUNISTIC) {
      metrics.startOpportunisticContainer(container.getResource());
    }

    long queueTime = clock.getTime() - container.getContainerStartTime();
    if (container.getContainerTokenIdentifier().getExecutionType()
        == ExecutionType.GUARANTEED) {
      metrics.addGuaranteedQueueLatencyEntry(queueTime);
    } else {
      metrics.addOpportunisticQueueLatencyEntry(queueTime);
    }
    container.sendLaunchEvent();
  }
}
