using System;

namespace CassandraEventStore.Domain
{
    public class Event
    {
        public Guid AggregateId { get; set; }
        public int Version { get; set; }
        public byte[] Data { get; set; }
        public DateTime Date { get; set; }
    }
}
