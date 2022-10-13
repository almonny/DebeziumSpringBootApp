package com.demo.debeziumspringbootapp;

import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.log.Slf4JLogger;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.data.Envelope;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.relational.history.FileDatabaseHistory;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.debezium.data.Envelope.FieldName.*;

@Service
public class DBEventsListener {

    private static final int MINIMUM_DATA_CHANGES = 50;
    private static final int MY_SQL_CONNECTOR_IDENTIFIER = UUID.randomUUID().hashCode() & Integer.MAX_VALUE;

    private static final String SNAPSHOT_LAST = "last";
    private DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;


    private final ExecutorService dbListenerExecutor;


    private boolean isSnapshotCompleted;


    private Boolean isAlive;

    @Autowired
    protected DBEventsListener() {
        this.dbListenerExecutor = createExecutor(3, "DBEventsListenerExecutor");
    }

    @PostConstruct
    private void start() {
        try {
            startDebeziumEngine();
        } catch (Exception ex) {
            throw new RuntimeException("failed to start DB events listener", ex);
        }
    }

    private void startDebeziumEngine() {
        debeziumEngine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(debeziumListenerConfig().asProperties())
                //informed when the engine terminates
                .using((success, message, throwable) -> {
                    if(!success && isAlive != null){
                        System.out.println(String.format("ERROR: debezium engine terminated with error. %s", message));
                        System.exit(1);
                    } else {
                       System.out.println( "debezium engine terminated");
                        destroy();
                    }
                })
                .using(new DebeziumConnectorCallback())
                .notifying(new DBChangeConsumer())
                .build();

        dbListenerExecutor.execute(debeziumEngine);
    }

    private Configuration debeziumListenerConfig() {
        Configuration.Builder configurationBuilder = Configuration.create()
                .with("name", "demo-mysql-connector")
                .with("connector.class", MySqlConnector.class)
                .with("offset.storage", FileOffsetBackingStore.class)
                .with("offset.storage.file.filename", "storageFile.dat")
                .with("offset.flush.interval.ms", Duration.ofSeconds(60).toMillis())
                /* begin connector properties */
                .with("database.hostname", "localHost")
                .with("database.port", 3306)
                .with("database.user", "*******************************************")
                .with("database.password", "****************************************")
                .with("database.server.id", MY_SQL_CONNECTOR_IDENTIFIER)
                .with("database.server.name", "demo_mysql_db_server")
                .with("database.history", FileDatabaseHistory.class)
                .with("heartbeat.interval.ms", Duration.ofSeconds(60).toMillis())
                .with("database.history.file.filename", "historyFile.dat")
                .with("database.connectionTimeZone", Strings.EMPTY) // Default ("Asia/Jerusalem")
                .with("column.include.list", Strings.EMPTY) // None
                .with("skipped.operations", Envelope.Operation.READ.code())
                .with("provide.transaction.metadata", false)
                .with("database.history.store.only.captured.tables.ddl", true)
                .with("include.schema.changes", true) //needed for truncate (not filtered by table.include.list)
                .with("poll.interval.ms", Duration.ofSeconds(20).toMillis()) //wait for new change events to appear before processing a batch
                .with("max.queue.size", 16384)
                .with("max.batch.size", 4096)
                .with("database.ssl.mode", PropertyDefinitions.SslMode.PREFERRED.name())
                .with("snapshot.mode", "schema_only"); //minimize startup history events
                configurationBuilder.with("database.logger", Slf4JLogger.class);
                configurationBuilder.with("database.profileSQL", true);


        Configuration config = configurationBuilder.build();

        System.out.println(String.format("Debezium configuration: %s", config));

        return config;
    }

    class DBChangeConsumer implements DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>> {

        private class Fields {
            private static final String PRIMARY_KEY = "id";
            private static final String DDL_COMMAND = "ddl";
            private static final String TABLE = "table";
            private static final String SNAPSHOT = "snapshot";
            private static final String SCHEMA_CHANGE_IDENTIFIER = "io.debezium.connector.mysql.SchemaChangeValue";
            private static final String HEARTBEAT_IDENTIFIER = "io.debezium.connector.common.Heartbeat";
        }

        @Override
        public void handleBatch(List<RecordChangeEvent<SourceRecord>> recordChangeEvents, DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> recordChangeEventRecordCommitter) {
            try {
               System.out.println( String.format("got %d events in batch",
                        recordChangeEvents.stream().filter(recordChangeEvent -> recordChangeEvent.record().value() != null).count()));

                Set<String> tablesWithSchemaChanges = getTablesWithSchemaChanges(recordChangeEvents);

                Map<String, Set<Integer>> dataChangeEvents = aggregateDataChangeEvents(recordChangeEvents, recordChangeEventRecordCommitter, tablesWithSchemaChanges);
                //for convenience all changes (add/delete/update) treated as update
                //in addition, this also allow to avoid issues with order of events on the same entity
                dataChangeEvents.entrySet().forEach(event -> {
                   System.out.println( String.format("handle DB data change event: table %s, entities %s", event.getKey(), event.getValue()));
                });
                recordChangeEventRecordCommitter.markBatchFinished();
            } catch (InterruptedException e) {
                System.out.println("WARN handle batch of DB update events has been interrupted");
                Thread.currentThread().interrupt();
            } catch (Exception ex) {
                System.out.println(String.format("ERROR failed to handle batch of DB update events. error: %s", ex));
            }
        }

        private Map<String, Set<Integer>> aggregateDataChangeEvents(List<RecordChangeEvent<SourceRecord>> recordChangeEvents, DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> recordChangeEventRecordCommitter, Set<String> tablesWithSchemaChanges) throws InterruptedException {
            Map<String, Set<Integer>> dataChangeEvents = new HashMap<>();
            for (RecordChangeEvent<SourceRecord> recordChangeEvent : recordChangeEvents) {
                aggregateDataChangeEvent(tablesWithSchemaChanges, dataChangeEvents, recordChangeEvent);
                recordChangeEventRecordCommitter.markProcessed(recordChangeEvent);
            }
            return dataChangeEvents;
        }

        private void aggregateDataChangeEvent(Set<String> tablesWithSchemaChanges, Map<String, Set<Integer>> dataChangeEvents, RecordChangeEvent<SourceRecord> recordChangeEvent) {
            SourceRecord sourceRecord = recordChangeEvent.record();
            try {
                Struct sourceRecordChangeValue = (Struct) sourceRecord.value();
                if (sourceRecordChangeValue == null) {
                    return;
                }

                if (isHeartbeat(sourceRecord)) {
                    System.out.println("heartbeat record ...");
                    return;
                }

                if (isSnapshot(sourceRecordChangeValue)) {
                    System.out.println(String.format("skip snapshot event, ddl: %s", sourceRecordChangeValue.getString(Fields.DDL_COMMAND)));
                    return;
                }

                if (isSchemaChange(sourceRecord)) {
                    System.out.println("schema change record ...");
                    return;
                }

                processDataChangeEvent(tablesWithSchemaChanges, dataChangeEvents, sourceRecord, sourceRecordChangeValue);
            } catch (Exception ex) {
                System.out.println(String.format("failed to handle source record [%s], error: %s", sourceRecord.toString(), ex));
            }
        }

        private void processDataChangeEvent(Set<String> tablesWithSchemaChanges, Map<String, Set<Integer>> dataChangeEvents, SourceRecord sourceRecord, Struct sourceRecordChangeValue) {
            String table = sourceRecordChangeValue.getStruct(SOURCE).getString(Fields.TABLE);

            Integer entityId = ((Struct) sourceRecord.key()).getInt32(Fields.PRIMARY_KEY);
            Envelope.Operation changeOperation = extractDataChangeOperation(sourceRecordChangeValue);

            System.out.println(String.format("aggregate DB data change event: operation %s, entity id %s, entity table %s",
                    changeOperation == null ? "N/A" : changeOperation.toString(),
                    entityId,
                    table));
        }


        private Set<String> getTablesWithSchemaChanges(List<RecordChangeEvent<SourceRecord>> recordChangeEvents) {
            return recordChangeEvents.stream()
                    .map(RecordChangeEvent::record)
                    .filter(this::isSchemaChange)
                    .map(SourceRecord::value)
                    .filter(Objects::nonNull)
                    .map(Struct.class::cast)
                    .filter(sourceRecord -> !isSnapshot(sourceRecord))
                    .map(value -> value.getStruct(SOURCE))
                    .map(source -> source.getString(Fields.TABLE))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
        }

        private boolean isSchemaChange(SourceRecord sourceRecord) {
            return isSchemaValueMatchingChangeIdentifier(sourceRecord, Fields.SCHEMA_CHANGE_IDENTIFIER);
        }

        private boolean isHeartbeat(SourceRecord sourceRecord) {
            return isSchemaValueMatchingChangeIdentifier(sourceRecord, Fields.HEARTBEAT_IDENTIFIER);
        }

        private boolean isSchemaValueMatchingChangeIdentifier(SourceRecord sourceRecord, String changeIdentifier){
            try {
                return sourceRecord.valueSchema().name().contains(changeIdentifier);
            } catch (Exception ex) {
                System.out.println(String.format("ERROR failed to extract schema value field from source record [%s], error: %s", sourceRecord.toString(), ex));
            }
            return false;
        }

        private Envelope.Operation extractDataChangeOperation(Struct sourceRecordChangeValue) {
            try {
                return Envelope.Operation.forCode(sourceRecordChangeValue.getString(OPERATION));
            } catch (Exception ex) {
                System.out.println(String.format("WARN failed to extract operation from data change %s", sourceRecordChangeValue.toString()));
                return null;
            }
        }

        private boolean isSnapshot(Struct sourceRecordChangeValue) {
            if (isSnapshotCompleted) {
                return false;
            }

            try {
                Struct source = sourceRecordChangeValue.getStruct(SOURCE);
                if (source.toString().contains(Fields.SNAPSHOT + "=")) {
                    analyzeSnapshotMessage(source);
                    return true;
                }

               System.out.println( "got non-snapshot message. assuming snapshot has been completed");
                isSnapshotCompleted = true;
            } catch (Exception ex) {
                System.out.println(String.format("WARN failed to check snapshot status. this message will be treated as non-snapshot. Source Record = %s. error: %s",
                        sourceRecordChangeValue.toString(), ex));
            }
            return false;
        }

        private void analyzeSnapshotMessage(Struct sourceRecordChangeValue) {
            try {
                String snapshotValue = sourceRecordChangeValue.getString(Fields.SNAPSHOT);
                if (SNAPSHOT_LAST.equals(snapshotValue)) {
                   System.out.println( "snapshot completed");
                    isSnapshotCompleted = true;
                }
            } catch (Exception ex) {
                System.out.println(String.format("WARN failed to check status in snapshot message. message cannot be used to mark completion of snapshot phase. Source Record Value = %s",
                        sourceRecordChangeValue, ex));
            }
        }
    }





    @PreDestroy
    private void close(){
        try {
           System.out.println( "DB events listener is closing...");
            isAlive = false;
            if(debeziumEngine != null) {
                debeziumEngine.close();
            }
        } catch (IOException ex) {
            System.out.println(String.format("failed to stop debezium engine. error: %s", ex));
        }
    }

    class DebeziumConnectorCallback implements DebeziumEngine.ConnectorCallback {

        @Override
        public void connectorStarted() {
           System.out.println( "debezium connector started");
        }

        @Override
        public void connectorStopped() {
            System.out.println( "debezium connector stopped");
        }
    }



    private static ThreadPoolExecutor createExecutor(int numOfThreads, String executorName) {
        System.out.println(String.format("create executor %s with %d threads", executorName, numOfThreads));
        return new ThreadPoolExecutor(1,
                numOfThreads,
                1L,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>());
    }

    private void destroy() {
        try {
            System.out.println("destroy DBEventsListenerExecutor");
            dbListenerExecutor.shutdown();
            if (!dbListenerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                System.out.println("force shutdown of DBEventsListenerExecutor");
                dbListenerExecutor.shutdownNow();
            }
        } catch (InterruptedException e){
            System.out.println(String.format("DBEventsListenerExecutor interrupted. %s", e.getMessage()));
            Thread.currentThread().interrupt();
        }
    }
}
