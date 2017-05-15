using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;

namespace CassandraEventStore.Domain
{
    public class OrdinalInsertion : IInsertionStrategy
    {
        private readonly Table<Event> _events;
        private readonly Table<EventSource> _eventSources;

        public OrdinalInsertion(Table<Event> events, Table<EventSource> eventSources)
        {
            _events = events;
            _eventSources = eventSources;
        }

        public void InsertEventsForAggregate(List<Event> events, ISession session, int currentVersion, Guid aggregateId)
        {
            var toBeCompleted = new List<Task>();

            int initialVersion = currentVersion;
            foreach (var e in events)
            {
                e.Version = ++currentVersion;

                toBeCompleted.Add(_events.Insert(e).ExecuteAsync());
            }

            Task.WaitAll(toBeCompleted.ToArray());

            UpdateEventSourceVersion(initialVersion, initialVersion + events.Count, aggregateId);
        }

        private void UpdateEventSourceVersion(int initialVersion,
            int currentVersion, Guid aggregateId)
        {
            if (initialVersion == 0)
                _eventSources.Insert(new EventSource
                {
                    AggregateId = aggregateId,
                    Version = currentVersion
                })
                .Execute();
            else
                _eventSources.Where(x => x.AggregateId == aggregateId)
                .Select(x => new EventSource
                {
                    Version = currentVersion
                })
                .Update()
                .Execute();
        }
    }
}
