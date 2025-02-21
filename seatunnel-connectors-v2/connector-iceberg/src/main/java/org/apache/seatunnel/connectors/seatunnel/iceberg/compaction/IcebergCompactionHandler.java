/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.iceberg.compaction;

import org.apache.seatunnel.shade.com.google.common.collect.Iterables;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.CompactionAckSourceEvent;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.RecordWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.WriteResult;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.Tasks;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@Slf4j
public class IcebergCompactionHandler {

    public static final String COMPACTION_ACTION_END = "compaction_end";

    private final Table table;
    private final FileIO fileIO;
    private final RecordWriter writer;

    public IcebergCompactionHandler(Table table, FileIO fileIO, RecordWriter writer) {
        this.table = table;
        this.fileIO = fileIO;
        this.writer = writer;
    }

    public static void emitCompactionEndRecord(
            Collector<SeaTunnelRow> output, CompactionAckSourceEvent event) {
        if (event.isClosed()) {
            return;
        }
        SeaTunnelRow endRecord = new SeaTunnelRow(4);
        endRecord.setField(0, COMPACTION_ACTION_END);
        endRecord.setField(1, event.getTablePath().getFullName());
        endRecord.setField(2, event.getStartSnapshot());
        endRecord.setField(3, event.getSequenceNumbers());
        output.collect(endRecord);
    }

    public boolean isCompactionEndRecord(SeaTunnelRow element) {
        return StringUtils.equals(element.getField(0).toString(), COMPACTION_ACTION_END);
    }

    public void doCompaction(SeaTunnelRow element) throws IOException {
        Long startingSnapshotId = (Long) element.getField(element.getArity() - 2);
        Set<Long> sequenceNumbers = (Set<Long>) element.getField(element.getArity() - 1);
        List<DataFile> currentDataFiles = Lists.newArrayList();
        CloseableIterator<CombinedScanTask> combinedScanTasks =
                table.newScan().planTasks().iterator();
        while (combinedScanTasks.hasNext()) {
            CombinedScanTask combinedScanTask = combinedScanTasks.next();
            for (FileScanTask fileScanTask : combinedScanTask.files()) {
                long sequenceNumber = fileScanTask.file().dataSequenceNumber();
                if (!sequenceNumbers.contains(sequenceNumber)) {
                    continue;
                }
                currentDataFiles.add(fileScanTask.file());
            }
        }
        List<DataFile> addedDataFiles = Lists.newArrayList();
        List<WriteResult> writeResults = writer.complete();
        for (WriteResult result : writeResults) {
            addedDataFiles.addAll(result.getDataFiles());
        }
        replaceDataFiles(currentDataFiles, addedDataFiles, startingSnapshotId);
    }

    private void replaceDataFiles(
            Iterable<DataFile> deletedDataFiles,
            Iterable<DataFile> addedDataFiles,
            long startingSnapshotId) {
        try {
            doReplace(deletedDataFiles, addedDataFiles, startingSnapshotId);
        } catch (CommitStateUnknownException e) {
            log.warn("Commit state unknown, cannot clean up files that may have been committed", e);
            throw e;
        } catch (Exception e) {
            if (e instanceof CleanableFailure) {
                log.warn("Failed to commit rewrite, cleaning up rewritten files", e);
                Tasks.foreach(
                                Iterables.transform(
                                        addedDataFiles,
                                        contentFile -> contentFile.path().toString()))
                        .noRetry()
                        .suppressFailureWhenFinished()
                        .onFailure(
                                (location, exc) -> log.warn("Failed to delete: {}", location, exc))
                        .run(fileIO::deleteFile);
            }
            throw e;
        }
    }

    private void doReplace(
            Iterable<DataFile> deletedDataFiles,
            Iterable<DataFile> addedDataFiles,
            long startingSnapshotId) {
        RewriteFiles rewriteFiles = table.newRewrite().validateFromSnapshot(startingSnapshotId);
        for (DataFile dataFile : deletedDataFiles) {
            rewriteFiles.deleteFile(dataFile);
        }
        for (DataFile dataFile : addedDataFiles) {
            rewriteFiles.addFile(dataFile);
        }
        long sequenceNumber = table.snapshot(startingSnapshotId).sequenceNumber();
        rewriteFiles.dataSequenceNumber(sequenceNumber);
        rewriteFiles.commit();
    }
}
