namespace Zen.Graph.TimeTree
{
    public interface ITimeTreeServiceFactory
    {
        ITimeTreeService Create(TimeTreeConfiguration configuration = null);
    }
}