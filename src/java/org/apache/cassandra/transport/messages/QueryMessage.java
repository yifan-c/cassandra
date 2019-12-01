/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.transport.messages;

import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.audit.AuditLogEntry;
import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryOptionsFactory;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * A CQL query
 */
public class QueryMessage extends Message.Request
{
    public static final Message.Codec<QueryMessage> codec = new Message.Codec<QueryMessage>()
    {
        public QueryMessage decode(ByteBuf body, ProtocolVersion version)
        {
            String query = CBUtil.readLongString(body);
            return new QueryMessage(query, QueryOptionsFactory.codec.decode(body, version));
        }

        public void encode(QueryMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeLongString(msg.query, dest);
            if (version == ProtocolVersion.V1)
                CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
            else
                QueryOptionsFactory.codec.encode(msg.options, dest, version);
        }

        public int encodedSize(QueryMessage msg, ProtocolVersion version)
        {
            int size = CBUtil.sizeOfLongString(msg.query);

            if (version == ProtocolVersion.V1)
            {
                size += CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency());
            }
            else
            {
                size += QueryOptionsFactory.codec.encodedSize(msg.options, version);
            }
            return size;
        }
    };

    public final String query;
    public final QueryOptions options;

    public QueryMessage(String query, QueryOptions options)
    {
        super(Type.QUERY);
        this.query = query;
        this.options = options;
    }

    @Override
    protected boolean isTraceable()
    {
        return true;
    }

    @Override
    protected Message.Response execute(QueryState state, long queryStartNanoTime, boolean traceRequest)
    {
        AuditLogManager auditLogManager = AuditLogManager.getInstance();

        try
        {
            if (options.getPageSize() == 0)
                throw new ProtocolException("The page size cannot be 0");

            if (traceRequest)
                traceQuery(state);

            long queryStartTime = auditLogManager.isLoggingEnabled() ? System.currentTimeMillis() : 0L;

            Message.Response response = ClientState.getCQLQueryHandler().process(query, state, options, getCustomPayload(), queryStartNanoTime);

            if (auditLogManager.isLoggingEnabled())
                logSuccess(state, queryStartTime);

            if (options.skipMetadata() && response instanceof ResultMessage.Rows)
                ((ResultMessage.Rows)response).result.metadata.setSkipMetadata();

            return response;
        }
        catch (Exception e)
        {
            if (auditLogManager.isLoggingEnabled())
                logException(state, e);
            JVMStabilityInspector.inspectThrowable(e);
            if (!((e instanceof RequestValidationException) || (e instanceof RequestExecutionException)))
                logger.error("Unexpected error during query", e);
            return ErrorMessage.fromException(e);
        }
    }

    private void traceQuery(QueryState state)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put("query", query);
        if (options.getPageSize() > 0)
            builder.put("page_size", Integer.toString(options.getPageSize()));
        if (options.getConsistency() != null)
            builder.put("consistency_level", options.getConsistency().name());
        if (options.getSerialConsistency() != null)
            builder.put("serial_consistency_level", options.getSerialConsistency().name());

        Tracing.instance.begin("Execute CQL3 query", state.getClientAddress(), builder.build());
    }

    private void logSuccess(QueryState state, long queryStartTime)
    {
        // FIXME: we are parsing the statement twice if audit logging is enabled. Why?
        CQLStatement statement = QueryProcessor.parseStatement(query, state.getClientState());
        AuditLogEntry entry =
            new AuditLogEntry.Builder(state)
                             .setType(statement.getAuditLogContext().auditLogEntryType)
                             .setOperation(query)
                             .setTimestamp(queryStartTime)
                             .setScope(statement)
                             .setKeyspace(state, statement)
                             .setOptions(options)
                             .build();
        AuditLogManager.getInstance().log(entry);
    }

    private void logException(QueryState state, Exception e)
    {
        AuditLogEntry entry =
            new AuditLogEntry.Builder(state)
                             .setOperation(query)
                             .setOptions(options)
                             .build();
        AuditLogManager.getInstance().log(entry, e);
    }

    @Override
    public String toString()
    {
        return String.format("QUERY %s [pageSize = %d]", query, options.getPageSize());
    }
}
