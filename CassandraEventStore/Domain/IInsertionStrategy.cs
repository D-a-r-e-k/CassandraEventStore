
using System;
using System.Collections.Generic;
using Cassandra;

namespace CassandraEventStore.Domain
{
    public interface IInsertionStrategy
    {
        void InsertEventsForAggregate(List<Event> events, ISession session, int currentVersion, Guid aggregateId);
    }
}
