using System;
using System.Threading.Tasks;
using Neo4jClient;
using Xunit;

namespace Zen.Graph.TimeTree.IntegrationTests
{
    public class UnitTest1
    {
        [Fact]
        public Task Test1()
        {
            var graphClientFactory = new GraphClientFactory(NeoServerConfiguration.GetConfiguration(
                new Uri("http://localhost:7474"), "neo4j", "Password1"));
            var timeTreeFactory = new TimeTreeServiceFactory(graphClientFactory, new TimeTreeConfiguration());
            var timeTreeService = timeTreeFactory.Create();

            return timeTreeService.Get(DateTimeOffset.UtcNow);
        }
    }
}
