using System;
using System.Data;
using System.IO;
using Microsoft.AnalysisServices.AdomdClient;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;

namespace etl_olap
{
     class Program
    {
        static void Main(string[] args)
        {
            // Get environment variables
            // dotnet add package Azure.Storage.Blobs
            string olapConnectionString = Environment.GetEnvironmentVariable("OLAP_CONNECTION_STRING");
            string mdxQuery = Environment.GetEnvironmentVariable("MDX_QUERY");
            string azureBlobConnectionString = Environment.GetEnvironmentVariable("AZURE_BLOB_CONNECTION_STRING");
            string containerName = Environment.GetEnvironmentVariable("AZURE_BLOB_CONTAINER_NAME");
            string blobFileName = Environment.GetEnvironmentVariable("BLOB_FILE_NAME");

            // Fetch data from OLAP cube
            DataTable dataTable = FetchDataFromOLAP(olapConnectionString, mdxQuery);

            // Create CSV file
            string csvFilePath = WriteDataTableToCSV(dataTable);

            // Upload CSV file to Azure Blob Storage
            UploadCSVToAzureBlob(azureBlobConnectionString, containerName, csvFilePath, blobFileName);

            // Clean up the local CSV file after upload
            if (File.Exists(csvFilePath))
            {
                File.Delete(csvFilePath);
            }
        }

        static DataTable FetchDataFromOLAP(string connectionString, string mdxQuery)
        {
            DataTable dataTable = new DataTable();

            try
            {
                using (AdomdConnection connection = new AdomdConnection(connectionString))
                {
                    connection.Open();
                    using (AdomdCommand command = new AdomdCommand(mdxQuery, connection))
                    using (AdomdDataReader reader = command.ExecuteReader())
                    {
                        dataTable.Load(reader);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching data from OLAP: {ex.Message}");
            }

            return dataTable;
        }

        static string WriteDataTableToCSV(DataTable dataTable)
        {
            string filePath = Path.GetTempFileName(); // Temporary file path

            try
            {
                using (StreamWriter writer = new StreamWriter(filePath))
                {
                    // Write column headers
                    for (int i = 0; i < dataTable.Columns.Count; i++)
                    {
                        writer.Write(dataTable.Columns[i]);
                        if (i < dataTable.Columns.Count - 1)
                            writer.Write(",");
                    }
                    writer.WriteLine();

                    // Write rows
                    foreach (DataRow row in dataTable.Rows)
                    {
                        for (int i = 0; i < dataTable.Columns.Count; i++)
                        {
                            writer.Write(row[i].ToString());
                            if (i < dataTable.Columns.Count - 1)
                                writer.Write(",");
                        }
                        writer.WriteLine();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error writing to CSV: {ex.Message}");
            }

            return filePath;
        }

        static void UploadCSVToAzureBlob(string connectionString, string containerName, string filePath, string blobFileName)
        {
            try
            {
                // Create a BlobServiceClient to interact with Azure Blob Storage
                BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
                
                // Get the container client
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
                
                // Ensure the container exists
                containerClient.CreateIfNotExists();

                // Get the blob client for the file to upload
                BlobClient blobClient = containerClient.GetBlobClient(blobFileName);

                // Upload the file
                using (FileStream fileStream = File.OpenRead(filePath))
                {
                    blobClient.Upload(fileStream, true);  // 'true' overwrites any existing file
                }

                Console.WriteLine("CSV file uploaded to Azure Blob Storage successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error uploading file to Azure Blob Storage: {ex.Message}");
            }
        }
    }
}
