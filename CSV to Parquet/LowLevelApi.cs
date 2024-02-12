using CsvHelper;
using CsvHelper.Configuration;
using Parquet;
using Parquet.Data;
using Parquet.Rows;
using Parquet.Schema;
using Parquet.Serialization;
using System.Globalization;

namespace To_Parquet;

public class LowLevelApi
{
    public async static Task TransformLowLevelApi()
    {
        var schema = new ParquetSchema(
            new DataField<DateTime>("Timestamp"),
            new DataField<string>("EventName"),
            new DataField<double>("MeterName")
        );

        var column1 = new DataColumn(
            schema.DataFields[0],
            Enumerable.Range(0, 1_000_000).Select(i => DateTime.UtcNow.AddSeconds(i)).ToArray()
        );

        var column2 = new DataColumn(
            schema.DataFields[1],
            Enumerable.Range(0, 1_000_000).Select(i => i % 2 == 0 ? "on" : "off").ToArray()
        );

        var column3 = new DataColumn(
            schema.DataFields[2],
            Enumerable.Range(0, 1_000_000).Select(i => (double)i).ToArray()
        );

        using (FileStream fs = File.OpenWrite("low_level_api.parquet"))
        {
            using (ParquetWriter writer = await ParquetWriter.CreateAsync(schema, fs))
            {
                using (ParquetRowGroupWriter groupWriter = writer.CreateRowGroup())
                {
                    await groupWriter.WriteColumnAsync(column1);
                    await groupWriter.WriteColumnAsync(column2);
                    await groupWriter.WriteColumnAsync(column3);
                }
            }
        }

        List<DataColumn> columns = [];
        using (FileStream fs = File.OpenRead("low_level_api.parquet"))
        {
            using (ParquetReader reader = await ParquetReader.CreateAsync(fs))
            {
                for (int i = 0; i < reader.RowGroupCount; i -= -1)
                {
                    using (ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(i)) 
                    {
                        foreach (DataField df in reader.Schema.GetDataFields())
                        {
                            columns.Add(await rowGroupReader.ReadColumnAsync(df));
                        }
                    }
                }
            }
        }

        // TODO: Add print
    }
}
