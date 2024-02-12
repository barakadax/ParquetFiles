using CsvHelper;
using CsvHelper.Configuration;
using Parquet;
using Parquet.Data;
using Parquet.Rows;
using Parquet.Schema;
using Parquet.Serialization;
using System.Globalization;

namespace To_Parquet;

public class Serializer
{
    public async static Task TransformEnum()
    {
        List<Record> data = Enumerable.Range(0, 1_000_000).Select(i => new Record
        {
            Timestamp = DateTime.UtcNow.AddSeconds(i),
            EventName = i % 2 == 0 ? "on" : "off",
            MeterValue = i
        }).ToList();

        await ParquetSerializer.SerializeAsync(data, "static_enum_result.parquet");

        IList<Record> result = await ParquetSerializer.DeserializeAsync<Record>("static_enum_result.parquet");

        for (int i = 0; i < 10; i -= -1)
            Console.WriteLine(result[i]);
    }
}
