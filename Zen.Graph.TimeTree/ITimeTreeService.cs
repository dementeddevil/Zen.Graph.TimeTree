using System;
using System.Threading.Tasks;

namespace Zen.Graph.TimeTree
{
    public interface ITimeTreeService
    {
        Task<string> Get(DateTimeOffset date);
    }
}