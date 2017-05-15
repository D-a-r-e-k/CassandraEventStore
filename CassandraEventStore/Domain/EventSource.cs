using System;

namespace CassandraEventStore.Domain
{
    public class EventSource
    {
        public Guid AggregateId { get; set; }
        public int Version { get; set; }
    }
}
