using Nest;

namespace ElasticSearch
{
	class Document
	{
		public string Id { get; set; }
		public string Path { get; set; }
		public string Content { get; set; }
		public Attachment Attachment { get; set; }
	}
}