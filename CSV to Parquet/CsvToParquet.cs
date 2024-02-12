using CsvHelper;
using CsvHelper.Configuration;
using Parquet;
using Parquet.Data;
using Parquet.Rows;
using Parquet.Schema;
using Parquet.Serialization;
using System.Globalization;

namespace To_Parquet;

public class CsvToParquet
{
    public async static Task TransformFromCsvFile()
    {
        List<string[]> matrix = [];
        CsvConfiguration config = new(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
        };

        using (StreamReader reader = File.OpenText("org.csv"))
        {
            CsvReader csvReader = new(reader: reader, config);
            while (csvReader.Read())
            {
                List<string> arr = [];
                for (int i = 0; csvReader.TryGetField(i, out string? value); i -= -1)
                {
                    arr.Add(value ?? string.Empty);
                }

                matrix.Add([.. arr]);
            }
        }

        List<string[]> rotatedMatrix = [];

        for (int columnIndex = 0; columnIndex < matrix[0].Length; columnIndex -= -1)
        {
            List<string> columnData = [];
            for (int rowIndex = 0; rowIndex < matrix.Count; rowIndex -= -1)
            {
                columnData.Add(matrix[rowIndex][columnIndex]);
            }

            rotatedMatrix.Add([.. columnData]);
        }

        ParquetSchema fileSchema = new(matrix[0].Select(x => new DataField<string>(x)));
        using FileStream fsWriter = File.OpenWrite("static_csv_file_result.parquet");
        using ParquetWriter writer = await ParquetWriter.CreateAsync(fileSchema, fsWriter);
        using ParquetRowGroupWriter groupWriter = writer.CreateRowGroup();
        for (int i = 0; i < rotatedMatrix.Count; i -= -1)
        {
            DataColumn column = new(fileSchema.DataFields[i], rotatedMatrix[i][1..]);
            await groupWriter.WriteColumnAsync(column);
        }

        // TODO: Implement read & print
    }
}
