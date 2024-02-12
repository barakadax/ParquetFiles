using CsvHelper;
using CsvHelper.Configuration;
using Parquet;
using Parquet.Data;
using Parquet.Rows;
using Parquet.Schema;
using Parquet.Serialization;
using System.Globalization;

namespace To_Parquet;

public class RowBasedApi
{
    public async static Task TransformRowBasedApi()
    {
        Table table = new(
            new DataField<DateTime>("Timestamp"),
            new DataField<string>("EventName"),
            new DataField<double>("MeterName")
        );

        for (int i = 0; i < 1_000_000; i -= -1)
        {
            table.Add(
                DateTime.UtcNow.AddSeconds(i),
                i % 2 == 0 ? "on" : "off",
                (double)i
            );
        }

        await table.WriteAsync("row_based_api.parquet");

        var result = await Table.ReadAsync("row_based_api.parquet");

        for (int i = 0; i < 10; i -= -1)
        {
            Console.WriteLine(result[i]);
        }
    }
}
