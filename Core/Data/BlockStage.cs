namespace AspNetCore.Kafka.Data
{
    public enum BlockStage
    {
        Message,
        Transform,
        Commit,
    }
}