using System;
using System.Threading.Tasks;
using Neo4jClient;
using Xunit;

namespace Zen.Graph.TimeTree.IntegrationTests
{
    public class UnitTest1
    {
        [Fact(DisplayName = "Add 100 date entries")]
        public async Task Test1()
        {
            var graphClientFactory = new GraphClientFactory(
                NeoServerConfiguration.GetConfiguration(
                    new Uri("http://localhost:32772/db/data"),
                    "neo4j",
                    "Password1"));
            var timeTreeFactory = new TimeTreeServiceFactory(graphClientFactory, new TimeTreeConfiguration());
            var timeTreeService = timeTreeFactory.Create();

            var date = DateTimeOffset.UtcNow;
            for (var index = 0; index < 100; ++index)
            {
                await timeTreeService.Get(date).ConfigureAwait(true);
                date = date.AddSeconds(18).AddMinutes(14).AddHours(1.3).AddDays(2.3);
            }
        }
    }
}
