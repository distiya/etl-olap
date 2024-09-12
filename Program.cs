using System;
using System.Data;
using Microsoft.AnalysisServices.AdomdClient;
using MongoDB.Bson;
using MongoDB.Driver;

namespace etl_olap
{
      class Program
    {
        static void Main(string[] args)
        {
            // Get environment variables
            string olapConnectionString = Environment.GetEnvironmentVariable("OLAP_CONNECTION_STRING");
            string mdxQuery = Environment.GetEnvironmentVariable("MDX_QUERY");
            string mongoConnectionString = Environment.GetEnvironmentVariable("MONGO_CONNECTION_STRING");
            string mongoDatabaseName = Environment.GetEnvironmentVariable("MONGO_DATABASE_NAME");
            string mongoCollectionName = Environment.GetEnvironmentVariable("MONGO_COLLECTION_NAME");

            // Fetch data from OLAP cube
            DataTable dataTable = FetchDataFromOLAP(olapConnectionString, mdxQuery);

            // Insert data into MongoDB
            InsertDataIntoMongoDB(mongoConnectionString, mongoDatabaseName, mongoCollectionName, dataTable);
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

        static void InsertDataIntoMongoDB(string mongoConnectionString, string databaseName, string collectionName, DataTable dataTable)
        {
            try
            {
                var client = new MongoClient(mongoConnectionString);
                var database = client.GetDatabase(databaseName);
                var collection = database.GetCollection<BsonDocument>(collectionName);

                foreach (DataRow row in dataTable.Rows)
                {
                    var document = new BsonDocument();
                    foreach (DataColumn column in dataTable.Columns)
                    {
                        document.Add(column.ColumnName, BsonValue.Create(row[column]));
                    }

                    collection.InsertOne(document);
                }

                Console.WriteLine("Data successfully inserted into MongoDB.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting data into MongoDB: {ex.Message}");
            }
        }
    }
}
