package pro.taskana.camunda.scheduler;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import pro.taskana.Task;
import pro.taskana.camunda.camundasystemconnector.api.CamundaSystemConnector;
import pro.taskana.camunda.camundasystemconnector.api.CamundaTask;
import pro.taskana.camunda.camundasystemconnector.spi.CamundaSystemConnectorProvider;
import pro.taskana.camunda.configuration.AdapterSchemaCreator;
import pro.taskana.camunda.configuration.RestClientConfiguration;
import pro.taskana.camunda.exceptions.TaskConversionFailedException;
import pro.taskana.camunda.exceptions.TaskCreationFailedException;
import pro.taskana.camunda.mappings.TimestampMapper;
import pro.taskana.camunda.taskanasystemconnector.api.TaskanaSystemConnector;
import pro.taskana.camunda.taskanasystemconnector.spi.TaskanaSystemConnectorProvider;
import pro.taskana.camunda.util.Assert;
import pro.taskana.impl.util.LoggerUtils;

/**
 * Scheduler for receiving Camunda tasks and completing Taskana tasks.
 *
 * @author kkl
 */
@Component
public class Scheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);
    private static final int TOTAL_TRANSACTON_LIFE_TIME = 125;

    @Autowired
    private RestClientConfiguration clientCfg;

    @Autowired
    private TimestampMapper timestampMapper;

    private Map<String, CamundaSystemConnector> camundaSystemConnectors;
    private List<TaskanaSystemConnector> taskanaSystemConnectors;


    @Scheduled(fixedRate = 5000)
    public void createTaskanaTasksFromCamundaTasks() {
        LOGGER.error("----------createTaskanaTasksFromCamundaTasks started----------------------------");
        for (CamundaSystemConnector connector : (camundaSystemConnectors.values())) {
            Instant lastRetrieved = timestampMapper.getLatestCreatedTimestamp(connector.getCamundaSystemURL());
            LOGGER.info("lastRetrieved is {}", lastRetrieved);

            Instant lastRetrievedMinusTransactionDuration;
            if (lastRetrieved != null) {
                lastRetrievedMinusTransactionDuration = lastRetrieved.minus(Duration.ofSeconds(TOTAL_TRANSACTON_LIFE_TIME));
            } else {
                lastRetrievedMinusTransactionDuration = Instant.now().minus(Duration.ofDays(365));
            }
            LOGGER.info("searching for tasks started after {}", lastRetrievedMinusTransactionDuration);

            List<CamundaTask> candidateTasks = connector.retrieveCamundaTasks(lastRetrievedMinusTransactionDuration);
            LOGGER.info("Candidate tasks retrieved from camunda: {}", LoggerUtils.listToString(candidateTasks));

            List<CamundaTask> tasksToStart = findNewTasksInListOfCandidateTasks(connector.getCamundaSystemURL(), candidateTasks);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("About to create taskana tasks for {} ", LoggerUtils.listToString(tasksToStart.stream().map(CamundaTask::getId)
                                                                                .collect(Collectors.toList())));
            }
            for (CamundaTask camundaTask : tasksToStart) {
                camundaTask.setCamundaSystemURL(connector.getCamundaSystemURL());
                createTaskanaTask(camundaTask);
            }
        }
    }

    @Transactional
    private void createTaskanaTask(CamundaTask camundaTask) {
        Assert.assertion(taskanaSystemConnectors.size() == 1, "taskanaSystemConnectors.size() == 1");
        TaskanaSystemConnector connector = taskanaSystemConnectors.get(0);
        try {
            Task taskanaTask = connector.convertToTaskanaTask(camundaTask);
            connector.createTaskanaTask(taskanaTask);
            timestampMapper.registerCreatedTask(camundaTask.getId(), Instant.now(), camundaTask.getCamundaSystemURL());
        } catch (TaskCreationFailedException | TaskConversionFailedException e) {
            LOGGER.error("Caught {} when creating a task in taskana for camunda task {}", e, camundaTask);
        }
    }

    private List<CamundaTask> findNewTasksInListOfCandidateTasks(String camundaSystemURL, List<CamundaTask> candidateTasks) {
        if (candidateTasks == null || candidateTasks.isEmpty()) {
            return candidateTasks;
        }
        List<String> candidateTaskIds = candidateTasks.stream().map(CamundaTask::getId).collect(Collectors.toList());
        List<String> existingTaskIds = timestampMapper.findExistingTaskIds(camundaSystemURL, candidateTaskIds);
        LOGGER.info("findNewTasks: candidate Tasks = \n {}", LoggerUtils.listToString(candidateTaskIds));
        LOGGER.info("findNewTasks: existing  Tasks = \n {}", LoggerUtils.listToString(existingTaskIds));

        List<String> newTaskIds = candidateTaskIds;
        newTaskIds.removeAll(existingTaskIds);
        LOGGER.info("findNewTasks: to create Tasks = \n {}", LoggerUtils.listToString(newTaskIds));

        return candidateTasks.stream()
            .filter(t -> newTaskIds.contains(t.getId()))
            .collect(Collectors.toList());
    }

    //  @Scheduled(fixedRate = 5000)
    public void completeCamundaTasks() {
        LOGGER.error("----------completeCamundaTasks started----------------------------");

        Assert.assertion(taskanaSystemConnectors.size() == 1, "taskanaSystemConnectors.size() == 1");
        Instant now = Instant.now();
        Instant lastRetrievedMinusTransactionDuration = timestampMapper.getLatestCompletedTimestamp();
        if (lastRetrievedMinusTransactionDuration == null) {
            lastRetrievedMinusTransactionDuration = now.minus(Duration.ofDays(1));
        } else {
            lastRetrievedMinusTransactionDuration = lastRetrievedMinusTransactionDuration.minus(Duration.ofSeconds(TOTAL_TRANSACTON_LIFE_TIME));
        }
        TaskanaSystemConnector taskanaSystemConnector = taskanaSystemConnectors.get(0);
        List<CamundaTask> candidateTasksCompletedByTaskana = taskanaSystemConnector.retrieveCompletedTaskanaTasks(lastRetrievedMinusTransactionDuration);
        List<CamundaTask> tasksToBeCompletedInCamunda = findTasksToBeCompletedInCamunda(candidateTasksCompletedByTaskana);
        for (CamundaTask camundaTask : tasksToBeCompletedInCamunda) {
            completeCamundaTask(camundaTask);
        }
    }

    private void completeCamundaTask(CamundaTask camundaTask) {
        CamundaSystemConnector connector = camundaSystemConnectors.get(camundaTask.getCamundaSystemURL());
        connector.completeCamundaTask(camundaTask.getId());
    }

    private List<CamundaTask> findTasksToBeCompletedInCamunda(List<CamundaTask> candidateTasksForCompletion) {
        List<String> candidateTaskIds = candidateTasksForCompletion.stream().map(CamundaTask::getId).collect(Collectors.toList());
        List<String> alreadyCompletedTaskIds = timestampMapper.findAlreadyCompletedTaskIds(candidateTaskIds);
        List<String> taskIdsToBeCompleted = candidateTaskIds;
        taskIdsToBeCompleted.removeAll(alreadyCompletedTaskIds);
        return candidateTasksForCompletion.stream()
            .filter(t -> taskIdsToBeCompleted.contains(t.getId()))
            .collect(Collectors.toList());
    }

    private void initSystemProviders() {
        initCamundaSystemConnectors();
        initTaskanaSystemConnectors();
    }

    private void initTaskanaSystemConnectors() {
        taskanaSystemConnectors = new ArrayList<>();
        ServiceLoader<TaskanaSystemConnectorProvider> loader = ServiceLoader.load(TaskanaSystemConnectorProvider.class);
        for (TaskanaSystemConnectorProvider provider : loader) {
            List<TaskanaSystemConnector> connectors = provider.create();
            taskanaSystemConnectors.addAll(connectors);
        }
    }

    private void initCamundaSystemConnectors() {
        camundaSystemConnectors = new HashMap<>();
        ServiceLoader<CamundaSystemConnectorProvider> loader = ServiceLoader.load(CamundaSystemConnectorProvider.class);
        for (CamundaSystemConnectorProvider provider : loader) {
            List<CamundaSystemConnector> connectors = provider.create();
            for (CamundaSystemConnector conn : connectors) {
                camundaSystemConnectors.put(conn.getCamundaSystemURL(), conn);
            }
        }
    }

    @PostConstruct
    private void init() {
        initSystemProviders();
        initDatabase();
    }

    private void initDatabase() {
        AdapterSchemaCreator schemaCreator = new AdapterSchemaCreator(clientCfg.dataSource(clientCfg.dataSourceProperties()), "TKA");
        try {
            schemaCreator.run();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
