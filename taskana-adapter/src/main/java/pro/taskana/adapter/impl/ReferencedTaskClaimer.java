package pro.taskana.adapter.impl;

import static org.springframework.transaction.annotation.Propagation.REQUIRES_NEW;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import pro.taskana.adapter.manager.AdapterManager;
import pro.taskana.adapter.systemconnector.api.ReferencedTask;
import pro.taskana.adapter.systemconnector.api.SystemConnector;
import pro.taskana.adapter.taskanaconnector.api.TaskanaConnector;
import pro.taskana.common.api.exceptions.SystemException;
import pro.taskana.task.api.CallbackState;

/**
 * Claims ReferencedTasks in external system that have been claimed in TASKANA.
 */
@Component
public class ReferencedTaskClaimer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReferencedTaskClaimer.class);

  //@Value("${taskana.adapter.run-as.user}")
  protected String runAsUser;

  AdapterManager adapterManager;

  private PlatformTransactionManager transactionManager;

  private TransactionTemplate transactionTemplate;

  @Autowired
  public ReferencedTaskClaimer(@Value("${taskana.adapter.run-as.user}") String runAsUser,
      AdapterManager adapterManager, PlatformTransactionManager transactionManager) {
    this.runAsUser = runAsUser;
    this.adapterManager = adapterManager;
    transactionTemplate = new TransactionTemplate(transactionManager);
    transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
    transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
  }

  @Scheduled(
      fixedRateString =
          "${taskana.adapter.scheduler.run.interval.for.claim.referenced.tasks."
              + "in.milliseconds:5000}")
  public void retrieveClaimedTaskanaTasksAndClaimCorrespondingReferencedTasks() {

    synchronized (ReferencedTaskClaimer.class) {
      if (!adapterManager.isInitialized()) {
        return;
      }

      LOGGER.debug(
          "--retrieveClaimedTaskanaTasksAndClaimCorrespondingReferencedTasks started-----------");
      try {
        UserContext.runAsUser(
            runAsUser,
            () -> {
              transactionTemplate.executeWithoutResult(status ->
                  retrieveClaimedTaskanaTasksAndClaimCorrespondingReferencedTask());
              return null;
            });
      } catch (Exception ex) {
        LOGGER.debug("Caught exception while trying to claim referenced tasks", ex);
      }
    }
  }

  @Transactional(rollbackFor = Exception.class, propagation = REQUIRES_NEW,
      isolation = Isolation.REPEATABLE_READ)
  public void retrieveClaimedTaskanaTasksAndClaimCorrespondingReferencedTask() {
    System.out.println("Start retrieving claimed tasks");
    LOGGER.trace(
        "ReferencedTaskClaimer."
            + "retrieveClaimedTaskanaTasksAndClaimCorrespondingReferencedTask ENTRY");
    try {
      TaskanaConnector taskanaSystemConnector = adapterManager.getTaskanaConnector();
      List<ReferencedTask> tasksClaimedByTaskana =
          taskanaSystemConnector.retrieveClaimedTaskanaTasksAsReferencedTasks();
      System.out.println("Retrieved " + tasksClaimedByTaskana.get(0).getId());
      List<ReferencedTask> tasksClaimedInExternalSystem =
          claimReferencedTasksInExternalSystem(tasksClaimedByTaskana);

      taskanaSystemConnector.changeTaskCallbackState(
          tasksClaimedInExternalSystem, CallbackState.CLAIMED);
      System.out.println("Changed callback state " + tasksClaimedByTaskana.get(0).getId());

    } finally {
      LOGGER.trace(
          "ReferencedTaskClaimer."
              + "retrieveClaimedTaskanaTasksAndClaimCorrespondingReferencedTask EXIT ");
    }
  }

  private List<ReferencedTask> claimReferencedTasksInExternalSystem(
      List<ReferencedTask> tasksClaimedByTaskana) {

    List<ReferencedTask> tasksClaimedInExternalSystem = new ArrayList<>();
    for (ReferencedTask referencedTask : tasksClaimedByTaskana) {
      if (claimReferencedTask(referencedTask)) {
        tasksClaimedInExternalSystem.add(referencedTask);
      }
    }
    return tasksClaimedInExternalSystem;
  }

  private boolean claimReferencedTask(ReferencedTask referencedTask) {
    LOGGER.trace(
        "ENTRY to ReferencedTaskClaimer.claimReferencedTask, TaskId = {} ", referencedTask.getId());
    boolean success = false;
    try {
      SystemConnector connector =
          adapterManager.getSystemConnectors().get(referencedTask.getSystemUrl());
      if (connector != null) {
        connector.claimReferencedTask(referencedTask);
        success = true;
      } else {
        throw new SystemException(
            "couldnt find a connector for systemUrl " + referencedTask.getSystemUrl());
      }
    } catch (Exception ex) {
      LOGGER.error("Caught {} when attempting to claim referenced task {}", ex, referencedTask);
    }
    LOGGER.trace("Exit from ReferencedTaskClaimer.claimReferencedTask, Success = {} ", success);
    return success;
  }
}
