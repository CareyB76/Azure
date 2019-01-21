using Microsoft.Azure;
using Microsoft.Azure.CosmosDB.Table;
using Microsoft.Azure.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TableStorage.Model;

namespace AzTableUtil
{
	public class AzTableUtil
	{
		public static CloudStorageAccount CreateStorageAccountFromConnectionString(string storageConnectionString)
		{
			CloudStorageAccount storageAccount;
			try
			{
				storageAccount = CloudStorageAccount.Parse(storageConnectionString);
			}
			catch (FormatException)
			{
				Console.WriteLine("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the app.config file - then restart the application.");
				throw;
			}
			catch (ArgumentException)
			{
				Console.WriteLine("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the app.config file - then restart the sample.");
				Console.ReadLine();
				throw;
			}

			return storageAccount;
		}
		public static CloudTable ConnectToTable(string tableName)
		{
			// Retrieve storage account information from connection string.
			CloudStorageAccount storageAccount = CreateStorageAccountFromConnectionString(CloudConfigurationManager.GetSetting("StorageConnectionString"));

			// Create a table client for interacting with the table service
			CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

			// Create a table client for interacting with the table service 
			CloudTable table = tableClient.GetTableReference(tableName);
			return table;
		}
		public static async Task DeletePartitionKeyAsync(CloudTable table, string partitionKey)
		{
			try
			{
				// Create the range query using the fluid API 
				TableQuery<TableEntity> rangeQuery = new TableQuery<TableEntity>().Where(
							TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey)
				);

				TableContinuationToken token = null;
				rangeQuery.TakeCount = 100;
				int segmentNumber = 0;
				do
				{
					TableQuerySegment<TableEntity> segment = await table.ExecuteQuerySegmentedAsync(rangeQuery, token);

					if (segment.Results.Count > 0)
					{
						segmentNumber++;
						Console.WriteLine();
						Console.WriteLine("Deleting segment {0}", segmentNumber);
						IList<ITableEntity> deleteThese = segment.ToList<ITableEntity>();
						DeleteBatch(deleteThese,table);
					}
					// Save the continuation token for the next call to ExecuteQuerySegmentedAsync
					token = segment.ContinuationToken;
				}
				while (token != null);
			}
			catch (StorageException e)
			{
				Console.WriteLine(e.Message);
				Console.ReadLine();
				throw;
			}
		}
		private static void DeleteBatch(IList<ITableEntity> toDelete,CloudTable table)
		{
			if (toDelete == null)
				throw new ArgumentNullException("toDelete");
			if (toDelete.Count==0)
				throw new ArgumentException("There is no elements in toDelete.");
			if (toDelete.GroupBy(e => e.PartitionKey).Count() > 1)
				throw new ArgumentException("The entities to delete must have the same PartitionKey.");
			
			Parallel.ForEach(Partitioner.Create(0, toDelete.Count, 100),
							 range =>
							 {
								 TableBatchOperation batchOperation = new TableBatchOperation();
								 for (Int32 i = range.Item1; i < range.Item2; i++)
									 batchOperation.Delete(toDelete[i]);
								 table.ExecuteBatch(batchOperation);
							 });
		}


	}


}
