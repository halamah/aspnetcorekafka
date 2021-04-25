using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.SchemaRegistry;

namespace AspNetCore.Kafka.Mock.InMemory
{
    public class MemorySchemaRegistry : ISchemaRegistryClient
    {
        public void Dispose()
        {
            throw new System.NotImplementedException();
        }

        public Task<int> RegisterSchemaAsync(string subject, string avroSchema)
        {
            throw new System.NotImplementedException();
        }

        public Task<int> RegisterSchemaAsync(string subject, Schema schema)
        {
            throw new System.NotImplementedException();
        }

        public Task<int> GetSchemaIdAsync(string subject, string avroSchema)
        {
            throw new System.NotImplementedException();
        }

        public Task<int> GetSchemaIdAsync(string subject, Schema schema)
        {
            throw new System.NotImplementedException();
        }

        public Task<Schema> GetSchemaAsync(int id, string format = null)
        {
            throw new System.NotImplementedException();
        }

        public Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool ignoreDeletedSchemas)
        {
            throw new System.NotImplementedException();
        }

        public Task<RegisteredSchema> GetRegisteredSchemaAsync(string subject, int version)
        {
            throw new System.NotImplementedException();
        }

        public Task<string> GetSchemaAsync(string subject, int version)
        {
            throw new System.NotImplementedException();
        }

        public Task<RegisteredSchema> GetLatestSchemaAsync(string subject)
        {
            throw new System.NotImplementedException();
        }

        public Task<List<string>> GetAllSubjectsAsync()
        {
            throw new System.NotImplementedException();
        }

        public Task<List<int>> GetSubjectVersionsAsync(string subject)
        {
            throw new System.NotImplementedException();
        }

        public Task<bool> IsCompatibleAsync(string subject, string avroSchema)
        {
            throw new System.NotImplementedException();
        }

        public Task<bool> IsCompatibleAsync(string subject, Schema schema)
        {
            throw new System.NotImplementedException();
        }

        public string ConstructKeySubjectName(string topic, string recordType = null)
        {
            throw new System.NotImplementedException();
        }

        public string ConstructValueSubjectName(string topic, string recordType = null)
        {
            throw new System.NotImplementedException();
        }

        public int MaxCachedSchemas { get; }
    }
}