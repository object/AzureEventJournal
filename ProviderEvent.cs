using System;

namespace AzureEventJournal
{
    public class ProviderEvent
    {
        public string Id { get; set; }
        public string Description { get; set; }
        public string Content { get; set; }
        public DateTime Created { get; set; }

        public ProviderEvent()
        {
        }

        public ProviderEvent(ProviderEvent other)
        {
            Id = other.Id;
            Description = other.Description;
            Content = other.Content;
            Created = other.Created;
        }
    }
}
