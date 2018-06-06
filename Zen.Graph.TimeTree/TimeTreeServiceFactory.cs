using Neo4jClient;

namespace Zen.Graph.TimeTree
{
    public class TimeTreeServiceFactory : ITimeTreeServiceFactory
    {
        private readonly IGraphClientFactory _graphClientFactory;
        private readonly TimeTreeConfiguration _defaultConfiguration;

        public TimeTreeServiceFactory(
            IGraphClientFactory graphClientFactory,
            TimeTreeConfiguration defaultConfiguration)
        {
            _graphClientFactory = graphClientFactory;
            _defaultConfiguration = defaultConfiguration;
        }

        public ITimeTreeService Create(TimeTreeConfiguration configuration = null)
        {
            return new TimeTreeService(_graphClientFactory, configuration ?? _defaultConfiguration);
        }
    }
}