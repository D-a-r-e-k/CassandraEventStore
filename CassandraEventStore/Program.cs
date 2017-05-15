using System;
using System.Collections.Generic;
using CassandraEventStore.Domain;
using CassandraEventStore.Utils;

namespace CassandraEventStore
{
    class Program
    {
        static void Main(string[] args)
        {
            var aggregateId = new Guid("0eaf40de-6481-4895-a654-e6bea1c6a594");

            using (var eventStore = new EventStore.EventStore())
            {
                var eventsToBeInserted = new List<Event>
                {
                    new Event()
                    {
                        AggregateId = aggregateId,
                        Version = 0,
                        Data = ProtoSerializer.Serialize("event1"),
                        Date = DateTime.Now
                    },
                    new Event()
                    {
                        AggregateId = aggregateId,
                        Version = 0,
                        Data = ProtoSerializer.Serialize("event2"),
                        Date = DateTime.Now
                    },
                    new Event()
                    {
                        AggregateId = aggregateId,
                        Version = 0,
                        Data = ProtoSerializer.Serialize("event3"),
                        Date = DateTime.Now
                    },
                    new Event()
                    {
                        AggregateId = aggregateId,
                        Version = 0,
                        Data = ProtoSerializer.Serialize("event4"),
                        Date = DateTime.Now
                    },
                    new Event()
                    {
                        AggregateId = aggregateId,
                        Version = 0,
                        Data = ProtoSerializer.Serialize("event5"),
                        Date = DateTime.Now
                    },
                    new Event()
                    {
                        AggregateId = aggregateId,
                        Version = 0,
                        Data = ProtoSerializer.Serialize("event6"),
                        Date = DateTime.Now
                    },
                    new Event()
                    {
                        AggregateId = aggregateId,
                        Version = 0,
                        Data = ProtoSerializer.Serialize("event7"),
                        Date = DateTime.Now
                    },
                    new Event()
                    {
                        AggregateId = aggregateId,
                        Version = 0,
                        Data = ProtoSerializer.Serialize("event8"),
                        Date = DateTime.Now
                    },
                    new Event()
                    {
                        AggregateId = aggregateId,
                        Version = 0,
                        Data = ProtoSerializer.Serialize("event9"),
                        Date = DateTime.Now
                    },
                    new Event()
                    {
                        AggregateId = aggregateId,
                        Version = 0,
                        Data = ProtoSerializer.Serialize("event10"),
                        Date = DateTime.Now
                    }
                };

                //var eventsToBeInserted = new List<Event>
                //{
                //    new Event()
                //    {
                //        AggregateId = aggregateId,
                //        Version = 0,
                //        Data = ProtoSerializer.Serialize("event1"),
                //        Date = DateTime.Now
                //    },
                //    new Event()
                //    {
                //        AggregateId = aggregateId,
                //        Version = 0,
                //        Data = ProtoSerializer.Serialize("event2"),
                //        Date = DateTime.Now
                //    }
                //};

                //new Guid("0eaf40de-6481-4895-a654-e6bea1c6a594")
                for (int i = 0; i < 3162; ++i)
                {
                    aggregateId = Guid.NewGuid();

                    foreach (var e in eventsToBeInserted)
                        e.AggregateId = aggregateId;

                    int expectedVersion = 0;
                    eventStore.SaveEvents(aggregateId, expectedVersion,
                        eventsToBeInserted);
                }
            }

            Console.WriteLine("Work is done.");
        }
    }
}
