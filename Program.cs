using System;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using Nest;

namespace ElasticSearch
{
	class MainClass
	{
		private static ElasticClient client;
		private static string documentsIndex;
		public static void Main(string[] args)
		{
			try
			{
				var node = new Uri("http://localhost:9200");
				var settings = new ConnectionSettings(node);
				client = new ElasticClient(settings);
				documentsIndex = "jess_books";
				string roothpath = "/Users/rkc/Projects/pdf_index/";

				if (Directory.Exists(roothpath))
				{
					// This path is a directory
					ProcessDirectory(roothpath);
				}
				else
				{
					Console.WriteLine("{0} is not a valid file or directory.", roothpath);
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine("Error: {0}", ex.Message);
			}
		});
		
		static void ProcessDirectory(string targetDirectory,bool deleteFile=true, string fileType="*.pdf")
		{
			Console.WriteLine("Processing directory {0}",targetDirectory);
			// Process the list of files found in the directory.
			string[] fileEntries = Directory.GetFiles(targetDirectory, fileType);
			if (fileEntries.Length <= 0)
				Console.WriteLine("No files found.");
			foreach (string fileName in fileEntries)
				ProcessFile(fileName,deleteFile);

			// Recurse into subdirectories of this directory.
			string[] subdirectoryEntries = Directory.GetDirectories(targetDirectory);
			foreach (string subdirectory in subdirectoryEntries)
				ProcessDirectory(subdirectory,deleteFile);


			Console.WriteLine("ProcessDirectory process complete. Proceeding to search");
		}

		static void ProcessFile(string file, bool deletefile)
		{
			Console.WriteLine("Processing file {0}", file);

			//todo

			if(deletefile)File.Delete(file);
			Console.WriteLine("File {0} processed", file);
		}
}
