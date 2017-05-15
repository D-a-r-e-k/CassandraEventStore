
using System.IO;
using ProtoBuf;

namespace CassandraEventStore.Utils
{
    public static class ProtoSerializer
    {
        public static byte[] Serialize<T>(T obj)
        {
            var memoryStream = new MemoryStream();
            Serializer.Serialize<T>(memoryStream, obj);

            return memoryStream.ToArray();
        }

        public static T Deserialize<T>(byte[] data)
        {
            var memoryStream = new MemoryStream(data);

            return Serializer.Deserialize<T>(memoryStream);
        }
    }
}
