namespace To_Parquet;

record Record
{
    public DateTime Timestamp { get; set; }
    public string? EventName { get; set; }
    public double MeterValue { get; set; }
}
