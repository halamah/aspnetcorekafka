using System;

namespace Tests.Data
{
    public record SampleMessage
    {
        public Guid Id { get; set; }
        
        public int Index { get; set; }
    }
}