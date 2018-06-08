using System;
using System.Threading.Tasks;

namespace Zen.Graph.TimeTree
{
    public interface ITimeTreeService
    {
        Task<TimeTreeReference> Get(DateTimeOffset date);
    }

    public class TimeTreeReference
    {
        public TimeTreeReference(string nodeType, string uniqueId)
        {
            NodeType = nodeType;
            UniqueId = uniqueId;
        }

        public string NodeType { get; }

        public string UniqueId { get; }
    }
}