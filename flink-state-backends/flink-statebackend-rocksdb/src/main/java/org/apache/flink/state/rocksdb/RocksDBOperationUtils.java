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

package org.apache.flink.state.rocksdb;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.ICloseableRegistry;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.state.rocksdb.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ExportImportFilesMetaData;
import org.rocksdb.ImportColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.state.rocksdb.RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME;

/** Utils for RocksDB Operations. */
public class RocksDBOperationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBOperationUtils.class);

    public static RocksDB openDB(
            String path,
            List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors,
            List<ColumnFamilyHandle> stateColumnFamilyHandles,
            ColumnFamilyOptions columnFamilyOptions,
            DBOptions dbOptions)
            throws IOException {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                new ArrayList<>(1 + stateColumnFamilyDescriptors.size());

        // we add the required descriptor for the default CF in FIRST position, see
        // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
        columnFamilyDescriptors.add(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
        columnFamilyDescriptors.addAll(stateColumnFamilyDescriptors);

        RocksDB dbRef;

        try {
            // Ensure that the working directory exists and is a directory to make RocksDB happy
            File pathFile = new File(Preconditions.checkNotNull(path));

            if (!pathFile.exists() && !pathFile.mkdirs() && !pathFile.isDirectory()) {
                throw new IOException(
                        "Could not create working directory for RocksDB instance: " + path);
            }

            dbRef =
                    RocksDB.open(
                            Preconditions.checkNotNull(dbOptions),
                            path,
                            columnFamilyDescriptors,
                            stateColumnFamilyHandles);
        } catch (Exception e) {
            IOUtils.closeQuietly(columnFamilyOptions);
            columnFamilyDescriptors.forEach((cfd) -> IOUtils.closeQuietly(cfd.getOptions()));

            // improve error reporting on Windows
            throwExceptionIfPathLengthExceededOnWindows(path, e);

            throw new IOException("Error while opening RocksDB instance.", e);
        }

        // requested + default CF
        Preconditions.checkState(
                1 + stateColumnFamilyDescriptors.size() == stateColumnFamilyHandles.size(),
                "Not all requested column family handles have been created");
        return dbRef;
    }

    public static RocksIteratorWrapper getRocksIterator(
            RocksDB db, ColumnFamilyHandle columnFamilyHandle, ReadOptions readOptions) {
        return new RocksIteratorWrapper(db.newIterator(columnFamilyHandle, readOptions));
    }

    public static void registerKvStateInformation(
            Map<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
            RocksDBNativeMetricMonitor nativeMetricMonitor,
            String columnFamilyName,
            RocksDBKeyedStateBackend.RocksDbKvStateInfo registeredColumn) {

        kvStateInformation.put(columnFamilyName, registeredColumn);
        if (nativeMetricMonitor != null) {
            nativeMetricMonitor.registerColumnFamily(
                    columnFamilyName, registeredColumn.columnFamilyHandle);
        }
    }

    /**
     * Creates a state info from a new meta info to use with a k/v state.
     *
     * <p>Creates the column family for the state. Sets TTL compaction filter if {@code
     * ttlCompactFiltersManager} is not {@code null}.
     *
     * @param importFilesMetaData if not empty, we import the files specified in the metadata to the
     *     column family.
     */
    public static RocksDBKeyedStateBackend.RocksDbKvStateInfo createStateInfo(
            RegisteredStateMetaInfoBase metaInfoBase,
            RocksDB db,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            @Nullable RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nullable Long writeBufferManagerCapacity,
            List<ExportImportFilesMetaData> importFilesMetaData,
            ICloseableRegistry cancelStreamRegistryForRestore) {

        ColumnFamilyDescriptor columnFamilyDescriptor =
                createColumnFamilyDescriptor(
                        metaInfoBase,
                        columnFamilyOptionsFactory,
                        ttlCompactFiltersManager,
                        writeBufferManagerCapacity);

        try {
            ColumnFamilyHandle columnFamilyHandle =
                    createColumnFamily(
                            columnFamilyDescriptor,
                            db,
                            importFilesMetaData,
                            cancelStreamRegistryForRestore);
            return new RocksDBKeyedStateBackend.RocksDbKvStateInfo(
                    columnFamilyHandle, metaInfoBase);
        } catch (Exception ex) {
            IOUtils.closeQuietly(columnFamilyDescriptor.getOptions());
            throw new FlinkRuntimeException("Error creating ColumnFamilyHandle.", ex);
        }
    }

    /**
     * Create RocksDB-backed KV-state, including RocksDB ColumnFamily.
     *
     * @param cancelStreamRegistryForRestore {@link ICloseableRegistry#close closing} it interrupts
     *     KV state creation
     */
    public static RocksDBKeyedStateBackend.RocksDbKvStateInfo createStateInfo(
            RegisteredStateMetaInfoBase metaInfoBase,
            RocksDB db,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            @Nullable RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nullable Long writeBufferManagerCapacity,
            ICloseableRegistry cancelStreamRegistryForRestore) {
        return createStateInfo(
                metaInfoBase,
                db,
                columnFamilyOptionsFactory,
                ttlCompactFiltersManager,
                writeBufferManagerCapacity,
                Collections.emptyList(),
                cancelStreamRegistryForRestore);
    }

    /**
     * Creates a column descriptor for a state column family.
     *
     * <p>Sets TTL compaction filter if {@code ttlCompactFiltersManager} is not {@code null}.
     */
    public static ColumnFamilyDescriptor createColumnFamilyDescriptor(
            RegisteredStateMetaInfoBase metaInfoBase,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            @Nullable RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nullable Long writeBufferManagerCapacity) {

        byte[] nameBytes = metaInfoBase.getName().getBytes(ConfigConstants.DEFAULT_CHARSET);
        Preconditions.checkState(
                !Arrays.equals(RocksDB.DEFAULT_COLUMN_FAMILY, nameBytes),
                "The chosen state name 'default' collides with the name of the default column family!");

        ColumnFamilyOptions options =
                createColumnFamilyOptions(columnFamilyOptionsFactory, metaInfoBase.getName());

        if (ttlCompactFiltersManager != null) {
            ttlCompactFiltersManager.setAndRegisterCompactFilterIfStateTtl(metaInfoBase, options);
        }

        if (writeBufferManagerCapacity != null) {
            // It'd be great to perform the check earlier, e.g. when creating write buffer manager.
            // Unfortunately the check needs write buffer size that was just calculated.
            sanityCheckArenaBlockSize(
                    options.writeBufferSize(),
                    options.arenaBlockSize(),
                    writeBufferManagerCapacity);
        }

        return new ColumnFamilyDescriptor(nameBytes, options);
    }

    /**
     * Logs a warning if the arena block size is too high causing RocksDB to flush constantly.
     * Essentially, the condition <a
     * href="https://github.com/dataArtisans/frocksdb/blob/49bc897d5d768026f1eb816d960c1f2383396ef4/include/rocksdb/write_buffer_manager.h#L47">
     * here</a> will always be true.
     *
     * @param writeBufferSize the size of write buffer (bytes)
     * @param arenaBlockSizeConfigured the manually configured arena block size, zero or less means
     *     not configured
     * @param writeBufferManagerCapacity the size of the write buffer manager (bytes)
     * @return true if sanity check passes, false otherwise
     */
    static boolean sanityCheckArenaBlockSize(
            long writeBufferSize, long arenaBlockSizeConfigured, long writeBufferManagerCapacity) {

        long defaultArenaBlockSize =
                RocksDBMemoryControllerUtils.calculateRocksDBDefaultArenaBlockSize(writeBufferSize);
        long arenaBlockSize =
                arenaBlockSizeConfigured <= 0 ? defaultArenaBlockSize : arenaBlockSizeConfigured;
        long mutableLimit =
                RocksDBMemoryControllerUtils.calculateRocksDBMutableLimit(
                        writeBufferManagerCapacity);
        if (RocksDBMemoryControllerUtils.validateArenaBlockSize(arenaBlockSize, mutableLimit)) {
            return true;
        } else {
            LOG.warn(
                    "RocksDBStateBackend performance will be poor because of the current Flink memory configuration! "
                            + "RocksDB will flush memtable constantly, causing high IO and CPU. "
                            + "Typically the easiest fix is to increase task manager managed memory size. "
                            + "If running locally, see the parameter taskmanager.memory.managed.size. "
                            + "Details: arenaBlockSize {} > mutableLimit {} (writeBufferSize = {}, arenaBlockSizeConfigured = {},"
                            + " defaultArenaBlockSize = {}, writeBufferManagerCapacity = {})",
                    arenaBlockSize,
                    mutableLimit,
                    writeBufferSize,
                    arenaBlockSizeConfigured,
                    defaultArenaBlockSize,
                    writeBufferManagerCapacity);
            return false;
        }
    }

    public static ColumnFamilyOptions createColumnFamilyOptions(
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory, String stateName) {

        // ensure that we use the right merge operator, because other code relies on this
        return columnFamilyOptionsFactory
                .apply(stateName)
                .setMergeOperatorName(MERGE_OPERATOR_NAME);
    }

    public static ColumnFamilyHandle createColumnFamily(
            ColumnFamilyDescriptor columnDescriptor,
            RocksDB db,
            List<ExportImportFilesMetaData> importFilesMetaData,
            ICloseableRegistry cancelStreamRegistryForRestore)
            throws RocksDBException, InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            // abort recovery if the task thread was already interrupted
            // e.g. because the task was cancelled
            throw new InterruptedException("The thread was interrupted, aborting recovery");
        } else if (cancelStreamRegistryForRestore.isClosed()) {
            throw new CancelTaskException("The stream was closed, aborting recovery");
        }

        if (importFilesMetaData.isEmpty()) {
            return db.createColumnFamily(columnDescriptor);
        } else {
            try (ImportColumnFamilyOptions importColumnFamilyOptions =
                    new ImportColumnFamilyOptions().setMoveFiles(true)) {
                return db.createColumnFamilyWithImport(
                        columnDescriptor, importColumnFamilyOptions, importFilesMetaData);
            }
        }
    }

    public static void addColumnFamilyOptionsToCloseLater(
            List<ColumnFamilyOptions> columnFamilyOptions, ColumnFamilyHandle columnFamilyHandle) {
        try {
            // IMPORTANT NOTE: Do not call ColumnFamilyHandle#getDescriptor() just to judge if it
            // return null and then call it again when it return is not null. That will cause
            // task manager native memory used by RocksDB can't be released timely after job
            // restart.
            // The problem can find in : https://issues.apache.org/jira/browse/FLINK-21986
            if (columnFamilyHandle != null) {
                ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyHandle.getDescriptor();
                if (columnFamilyDescriptor != null) {
                    columnFamilyOptions.add(columnFamilyDescriptor.getOptions());
                }
            }
        } catch (RocksDBException e) {
            // ignore
        }
    }

    @Nullable
    public static OpaqueMemoryResource<RocksDBSharedResources> allocateSharedCachesIfConfigured(
            RocksDBMemoryConfiguration jobMemoryConfig,
            Environment env,
            double memoryFraction,
            Logger logger,
            RocksDBMemoryControllerUtils.RocksDBMemoryFactory rocksDBMemoryFactory)
            throws IOException {

        try {
            RocksDBSharedResourcesFactory factory =
                    RocksDBSharedResourcesFactory.from(jobMemoryConfig, env);
            if (factory == null) {
                return null;
            }

            return factory.create(
                    jobMemoryConfig, env, memoryFraction, logger, rocksDBMemoryFactory);

        } catch (Exception e) {
            throw new IOException("Failed to acquire shared cache resource for RocksDB", e);
        }
    }

    private static void throwExceptionIfPathLengthExceededOnWindows(String path, Exception cause)
            throws IOException {
        // max directory path length on Windows is 247.
        // the maximum path length is 260, subtracting one file name length (12 chars) and one NULL
        // terminator.
        final int maxWinDirPathLen = 247;

        if (path.length() > maxWinDirPathLen && OperatingSystem.isWindows()) {
            throw new IOException(
                    String.format(
                            "The directory path length (%d) is longer than the directory path length limit for Windows (%d): %s",
                            path.length(), maxWinDirPathLen, path),
                    cause);
        }
    }
}
