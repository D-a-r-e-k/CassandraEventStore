using System;
using System.Collections.Generic;
using Cassandra;
using Cassandra.Data.Linq;

namespace CassandraEventStore.Domain
{
    public class AtomicInsertion : IInsertionStrategy
    {
        private readonly Table<Event> _events;
        private readonly Table<EventSource> _eventSources;

        public AtomicInsertion(Table<Event> events, Table<EventSource> eventSources)
        {
            _events = events;
            _eventSources = eventSources;
        }

        public void InsertEventsForAggregate(List<Event> events, ISession session, int currentVersion, Guid aggregateId)
        {
            // batch is used in order to ensure atomicity
            var batch = session.CreateBatch();

            int initialVersion = currentVersion;

            foreach (var e in events)
            {
                e.Version = ++currentVersion;

                batch.Append(_events.Insert(e));
            }

            UpdateEventSourceVersion(batch, initialVersion, initialVersion + events.Count, aggregateId);

            batch.Execute();
        }

        private void UpdateEventSourceVersion(Batch batch, int initialVersion,
            int currentVersion, Guid aggregateId)
        {
            if (initialVersion == 0)
                batch.Append(
                    _eventSources.Insert(new EventSource()
                    {
                        AggregateId = aggregateId,
                        Version = currentVersion
                    }));
            else
                batch.Append(
                    _eventSources.Where(x => x.AggregateId == aggregateId)
                    .Select(x => new EventSource
                    {
                        Version = currentVersion
                    })
                    .Update());
        }
    }
}
