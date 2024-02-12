using To_Parquet;

namespace Parquet;

public class Program
{
    public async static Task Main()
    {
        //await Serializer.TransformEnum();
        //await CsvToParquet.TransformFromCsvFile();
        //await RowBasedApi.TransformRowBasedApi();
        await LowLevelApi.TransformLowLevelApi();
    }
}
