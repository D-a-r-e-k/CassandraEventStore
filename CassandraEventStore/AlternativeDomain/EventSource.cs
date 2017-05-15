
using System;

namespace CassandraEventStore.AlternativeDomain
{
    public class EventSource
    {
        public Guid AggregateId { get; set; }
        public int EventVersion { get; set; }
        public int EventSourceVersion { get; set; }
        public byte[] Data { get; set; }
        public DateTime Date { get; set; }
    }
}
