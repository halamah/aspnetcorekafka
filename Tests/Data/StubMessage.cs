using System;

namespace Tests.Data
{
    public record StubMessage
    {
        public Guid Id { get; set; }
        
        public int Index { get; set; }
    }
}