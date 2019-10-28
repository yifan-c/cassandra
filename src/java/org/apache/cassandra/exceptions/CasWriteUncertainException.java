package org.apache.cassandra.exceptions;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;

public class CasWriteUncertainException extends RequestExecutionException
{
    public final ConsistencyLevel consistency;
    public final int received;
    public final int blockFor;

    public CasWriteUncertainException(ConsistencyLevel consistency, int received, int blockFor)
    {
        super(ExceptionCode.CAS_UNCERTAINTY, String.format("Cas operation result is uncertain - proposal accepted by %d but not a quorum.", received));
        this.consistency = consistency;
        this.received = received;
        this.blockFor = blockFor;
    }
}
