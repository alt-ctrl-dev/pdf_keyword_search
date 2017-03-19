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
					CreateMapping();
					ProcessDirectory(roothpath);
					SearchKeyword("technology");
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

			var id = Guid.NewGuid().ToString();
			client.Index(new Document
			{
				Id =id,
				Path = file,
				Content = Convert.ToBase64String(File.ReadAllBytes(file))
			}, i => i.Pipeline("attachments").Index(documentsIndex).Id(id));

			if(deletefile)File.Delete(file);
			Console.WriteLine("File {0} processed", file);
		}
		
		static void CreateMapping()
		{
			var mappingResponse = client.GetMapping<Document>((arg) => arg.Index(documentsIndex));
			Console.WriteLine("Checking to see if mapping is required");
			if (!mappingResponse.Mappings.ContainsKey(documentsIndex))
			{
				Console.WriteLine("Creating mapping for {0}", documentsIndex);
				var indexResponse = client.CreateIndex(documentsIndex, c => c
				  .Settings(s => s
					.Analysis(a => a
					  .Analyzers(ad => ad
						.Custom("windows_path_hierarchy_analyzer", ca => ca
						  .Tokenizer("windows_path_hierarchy_tokenizer")
						)
					  )
					  .Tokenizers(t => t
						.PathHierarchy("windows_path_hierarchy_tokenizer", ph => ph
						  .Delimiter('\\')
						)
					  )
					)
				  )
				  .Mappings(m => m
					.Map<Document>(mp => mp
					  .AutoMap()
					  .AllField(all => all
						.Enabled(false)
					  )
					  .Properties(ps => ps
						.Text(s => s
						  .Name(n => n.Path)
						  .Analyzer("windows_path_hierarchy_analyzer")
						)
						.Object<Attachment>(a => a
						  .Name(n => n.Attachment)
						  .AutoMap()
						)
					  )
					)
				  )
				);

				client.PutPipeline("attachments", p => p
					.Description("Document attachment pipeline")
					.Processors(pr => pr
						.Attachment<Document>(a => a
							.Field(f => f.Content)
							.TargetField(f => f.Attachment)
						)
						.Remove<Document>(r => r
							.Field(f => f.Content)
						)
					)
				);
			}
			else Console.WriteLine("Mapping not required");
			Console.WriteLine("CreateMapping process complete");
		}
		
		static void SearchKeyword(string keyword)
		{
			Console.WriteLine("\n\n==============================================================\n"
			                  +"searching for keyword \"{0}\"",keyword);
			var searchResponse = client.Search<Document>(s => s
									  .Query(q => q
										.Match(m => m
										  .Field(a => a.Attachment.Content)
										  .Query(keyword)
										)
									  )
									 .Index(documentsIndex)
									);
			Console.WriteLine("Total possible matches found {0}", searchResponse.Total);
			Console.WriteLine("Total hits found {0}", searchResponse.Hits.Count);

			foreach (var hit in searchResponse.Hits)
			{
				Console.WriteLine("Hit ID: {0}\nHit score:{1:F3}\nFile:{2}\n\n", hit.Id,hit.Score,Path.GetFileName(hit.Source.Path));
			}
		}
}
