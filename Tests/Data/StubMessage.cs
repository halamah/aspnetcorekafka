using System;

namespace Tests.Data
{
    public record StubMessage
    {
        public Guid Id { get; set; } = Guid.NewGuid();
        
        public int Index { get; set; }
    }
}