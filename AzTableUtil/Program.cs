using Microsoft.Azure.CosmosDB.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzTableUtil
{
	class Program
	{
		static void Main(string[] args)
		{
			Dictionary<string, string> arguments = ParseArgs(args);
			string tableName = arguments["table"];
			string partitionKey= arguments["partitionkey"];

			CloudTable table = AzTableUtil.ConnectToTable(tableName);
			if (table.Exists())
			{
				Console.WriteLine("Table Exists");

				Task t = AzTableUtil.DeletePartitionKeyAsync(table, partitionKey);
				t.Wait();
			}
		}
		static Dictionary<string, string> ParseArgs(string[] args)
		{
			Dictionary<string, string> arguments = new Dictionary<string, string>();
			foreach (string arg in args)
			{
				// table=tableName PartitionKey=key
				string [] a = arg.Split('=');
				arguments.Add(a[0].ToLower(), a[1].Trim('"'));
			}

			return arguments;
		
		}
	}
}
