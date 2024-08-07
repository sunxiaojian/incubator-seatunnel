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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.List;

@AutoService(Factory.class)
public class TiDBIncrementalSourceFactory implements TableSourceFactory {
    /**
     * Returns a unique identifier among same factory interfaces.
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code
     * kafka}). If multiple factories exist for different versions, a version should be appended
     * using "-" (e.g. {@code elasticsearch-7}).
     */
    @Override
    public String factoryIdentifier() {
        return TiDBIncrementalSource.IDENTIFIER;
    }

    /**
     * Returns the rule for options.
     *
     * <p>1. Used to verify whether the parameters configured by the user conform to the rules of
     * the options;
     *
     * <p>2. Used for Web-UI to prompt user to configure option value;
     */
    @Override
    public OptionRule optionRule() {
        return TiDBSourceOptions.getBaseRule()
                .required(
                        TiDBSourceOptions.DATABASE_NAME,
                        TiDBSourceOptions.TABLE_NAME,
                        TiDBSourceOptions.PD_ADDRESSES)
                .optional(
                        TiDBSourceOptions.TIKV_BATCH_GET_CONCURRENCY,
                        TiDBSourceOptions.TIKV_BATCH_SCAN_CONCURRENCY,
                        TiDBSourceOptions.TIKV_GRPC_SCAN_TIMEOUT,
                        TiDBSourceOptions.TIKV_GRPC_TIMEOUT,
                        TiDBSourceOptions.STARTUP_MODE,
                        TiDBSourceOptions.STOP_MODE)
                .build();
    }

    /**
     * TODO: Implement SupportParallelism in the TableSourceFactory instead of the SeaTunnelSource,
     * Then deprecated the method
     */
    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return TiDBIncrementalSource.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            List<CatalogTable> catalogTables =
                    CatalogTableUtil.getCatalogTables(
                            context.getOptions(), context.getClassLoader());
            SeaTunnelDataType<SeaTunnelRow> dataType =
                    CatalogTableUtil.convertToMultipleRowType(catalogTables);
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new TiDBIncrementalSource<>(context.getOptions(), dataType, catalogTables);
        };
    }
}
