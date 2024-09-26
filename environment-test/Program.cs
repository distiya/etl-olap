using System;
using System.IO;
using System.Text.Json;

namespace environment_test
{
      class Program
    {
        static void Main(string[] args)
        {
            // Get environment variables
            string testUser = Environment.GetEnvironmentVariable("TEST_USER");
            string testPassword = Environment.GetEnvironmentVariable("TEST_PASSWORD");
            Console.WriteLine($"Test user is: {testUser}");
            Console.WriteLine($"Test password is: {testPassword}");
            OperationStatus operationStatus;
            if(testPassword == "batti"){
            	Console.WriteLine("Password is correct");
            	operationStatus = new OperationStatus
		{
		    status = 0,
		    fileName = "test.csv"
		};
            }
            else{
            	operationStatus = new OperationStatus
		{
		    status = 1
		};
            }
            string jsonString = JsonSerializer.Serialize(operationStatus);
            string filePath = "/airflow/xcom/return.json";
            File.WriteAllText(filePath, jsonString);
        }
    }
    
    class OperationStatus{
    	public int status { get; set; }
    	public string fileName { get; set; }
    }
}
