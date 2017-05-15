using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using CassandraEventStore.Domain;

namespace CassandraEventStore.EventStore
{
    public class EventStore : IDisposable
    {
        private const string SimpleKeySpace = "simple";

        private readonly ISession _session;

        private readonly Table<EventSource> _eventSources;

        private readonly IInsertionStrategy _insertionStrategy;

        private readonly Table<AlternativeDomain.EventSource> _columnOrientedEventSources;

        public EventStore()
        {
            Cluster cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
            _session = cluster.Connect();

            Configure();          

            var events = new Table<Event>(_session);
            _eventSources = new Table<EventSource>(_session);
            _columnOrientedEventSources = new Table<AlternativeDomain.EventSource>(_session);
            _insertionStrategy = new AtomicInsertion(events, _eventSources);
        }

        public IEnumerable<Event> GetEventsForAggregate(Guid aggregateId)
        {
            return _columnOrientedEventSources
                .Where(x => x.AggregateId == aggregateId)
                .Select(x => new Event
                {
                    AggregateId = x.AggregateId,
                    Date = x.Date,
                    Version = x.EventVersion,
                    Data = x.Data
                })
                .Execute();
        }

        public void SaveEvents(Guid aggregateId, int expectedVersion, List<Event> events)
        {
            int currentVersion = GetEventSourceVersionInColumnOrientedFashion(aggregateId);

            if (expectedVersion != currentVersion)
                throw new DBConcurrencyException("Concurrency problem.");

            var batch = _session.CreateBatch();
            foreach (var e in events
                .Select(x => new AlternativeDomain.EventSource()
                {
                    EventVersion = x.Version,
                    AggregateId = x.AggregateId,
                    Data = x.Data,
                    Date = x.Date
                }))
            {
                e.EventVersion = ++currentVersion;

                batch.Append(_columnOrientedEventSources.Insert(e));
            }
            
            batch.Append(_columnOrientedEventSources.Insert(new AlternativeDomain.EventSource()
            {
                EventSourceVersion = currentVersion,
                AggregateId = aggregateId
            }));

            batch.Execute();
        }

        public void SaveEventsNotAtomically(Guid aggregateId, int expectedVersion, List<Event> events)
        {
            int currentVersion = GetEventSourceVersionInColumnOrientedFashion(aggregateId);

            if (expectedVersion != currentVersion)
                throw new DBConcurrencyException("Concurrency problem.");

            var toBeCompleted = new List<Task>();
            foreach (var e in events
                .Select(x => new AlternativeDomain.EventSource()
                {
                    EventVersion = x.Version,
                    AggregateId = x.AggregateId,
                    Data = x.Data,
                    Date = x.Date
                }))
            {
                e.EventVersion = ++currentVersion;

                toBeCompleted.Add(_columnOrientedEventSources.Insert(e).ExecuteAsync());
            }

            toBeCompleted.Add(_columnOrientedEventSources.Insert(new AlternativeDomain.EventSource()
            {
                EventSourceVersion = currentVersion,
                AggregateId = aggregateId
            }).ExecuteAsync());

            Task.WaitAll(toBeCompleted.ToArray());
        }

        private int GetEventSourceVersionInColumnOrientedFashion(Guid aggreagateId)
        {
            var eventSource = _columnOrientedEventSources
                .FirstOrDefault(x => x.AggregateId == aggreagateId)
                .Execute();

            if (eventSource == null)
                return 0;

            return eventSource.EventSourceVersion;
        }

        #region Relational structure

        public void SaveEventsInRelationalManner(Guid aggregateId, int expectedVersion, List<Event> events)
        {
            int currentVersion = GetEventSourceVersion(aggregateId);

            if (expectedVersion != currentVersion)
                throw new DBConcurrencyException("Concurrency problem.");

            _insertionStrategy.InsertEventsForAggregate(events, _session, currentVersion, aggregateId);
        }

        private int GetEventSourceVersion(Guid aggreagateId)
        {
            var eventSource = _eventSources
                .FirstOrDefault(x => x.AggregateId == aggreagateId)
                .Execute();

            if (eventSource == null)
                return 0;

            return eventSource.Version;
        }

        #endregion


        private void Configure()
        {
            _session.CreateKeyspaceIfNotExists(SimpleKeySpace,
                new Dictionary<string, string>()
                {
                    { "class", "SimpleStrategy" },
                    { "replication_factor", "1" }
                });

            _session.ChangeKeyspace(SimpleKeySpace);

            MappingConfiguration.Global.Define(
                new Map<AlternativeDomain.EventSource>()
                    .TableName("EventSource2")
                    .KeyspaceName(SimpleKeySpace)
                    .PartitionKey(x => x.AggregateId)
                    .ClusteringKey(x => x.EventVersion)
                    .Column(x => x.Data, cm => cm.WithName("Data"))
                    .Column(x => x.Date, cm => cm.WithName("Date")));

            MappingConfiguration.Global.Define(
                new Map<Event>()
                    .TableName("Event")
                    .KeyspaceName(SimpleKeySpace)
                    .PartitionKey(x => x.AggregateId)
                    .ClusteringKey(x => x.Version)
                    .Column(x => x.Data, cm => cm.WithName("Data"))
                    .Column(x => x.Date, cm => cm.WithName("Date")));

            MappingConfiguration.Global.Define(
                new Map<EventSource>()
                    .TableName("EventSource")
                    .KeyspaceName(SimpleKeySpace)
                    .PartitionKey(x => x.AggregateId)
                    .Column(x => x.Version, cm => cm.WithName("Version")));
        }

        public void Dispose()
        {
            _session.Dispose();
        }
    }
}
