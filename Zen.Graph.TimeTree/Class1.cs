using System;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Serialization;
using Neo4jClient;

namespace Zen.Graph.TimeTree
{
    public class TimeTreeService
    {
        public class CalendarQuarter
        {
            public CalendarQuarter(DateTimeOffset date)
            {
                Quarter = ((date.Month - 1) / 3) + 1;
                Year = date.Year;
            }

            private CalendarQuarter(int quarter, int year)
            {
                while (quarter < 1)
                {
                    --year;
                    quarter += 4;
                }

                while (quarter> 4)
                {
                    ++year;
                    quarter -= 4;
                }

                Quarter = quarter;
                Year = year;
            }

            public int Quarter { get; }

            public int Year { get; }

            public CalendarQuarter Previous =>
                new CalendarQuarter(Quarter - 1, Year);

            public CalendarQuarter Next =>
                new CalendarQuarter(Quarter + 1, Year);
        }

        public class CalendarWeek
        {
            public CalendarWeek(DateTimeOffset date, DayOfWeek firstDayOfWeek)
            {
                Date = date;
                FirstDayOfWeek = firstDayOfWeek;

                WeekNumber = GetWeekNumber(date);
            }

            public DateTimeOffset Date { get; }

            public DayOfWeek FirstDayOfWeek { get; }

            public int WeekNumber { get; }

            public CalendarWeek Previous =>
                new CalendarWeek(Date.AddDays(-7), FirstDayOfWeek);

            public CalendarWeek Next =>
                new CalendarWeek(Date.AddDays(7), FirstDayOfWeek);

            private DateTimeOffset GetFirstDayWeekDay(DateTimeOffset reference)
            {
                var date = new DateTimeOffset(reference.Year, 1, 1, 0, 0, 0, 0, reference.Offset);
                while (date.DayOfWeek != FirstDayOfWeek)
                {
                    date = date.AddDays(1);
                }
                return date;
            }

            private int GetWeekNumber(DateTimeOffset date)
            {
                var diff = date - GetFirstDayWeekDay(date);
                if (diff < TimeSpan.Zero)
                {
                    diff = date - GetFirstDayWeekDay(date.AddYears(-1));
                }
                return (diff.Days / 7) + 1;
            }
        }

        private readonly TimeTreeConfiguration _configuration;
        private readonly IGraphClientFactory _graphClientFactory;

        public TimeTreeService(IGraphClientFactory graphClientFactory, TimeTreeConfiguration configuration)
        {
            _graphClientFactory = graphClientFactory;
            _configuration = configuration;
        }

        public async Task<string> Get(DateTimeOffset date)
        {
            var graphClient = _graphClientFactory.Create();

            // Create year (link if possible)
            var yearId = await GetYearIdentifierAsync(graphClient, date, true).ConfigureAwait(false);
            var prevYearId = await GetYearIdentifierAsync(graphClient, date.AddYears(-1), false).ConfigureAwait(false);
            var nextYearId = await GetYearIdentifierAsync(graphClient, date.AddYears(1), false).ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "PREV", "Year", prevYearId,
                    "NEXT", "Year", yearId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "PREV", "Year", yearId,
                    "NEXT", "Year", nextYearId)
                .ConfigureAwait(false);

            // Create quarter
            var quarter = new CalendarQuarter(date);
            var quarterId = await GetQuarterIdentifierAsync(graphClient, quarter, true).ConfigureAwait(false);
            var prevQuarterId = await GetQuarterIdentifierAsync(graphClient, quarter.Previous, false).ConfigureAwait(false);
            var nextQuarterId = await GetQuarterIdentifierAsync(graphClient, quarter.Next, false).ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "PREV", "Quarter", prevQuarterId,
                    "NEXT", "Quarter", quarterId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "PREV", "Quarter", quarterId,
                    "NEXT", "Quarter", nextQuarterId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "CONTAINED_BY", "Year", yearId,
                    "SEGMENTED_BY", "Quarter", quarterId)
                .ConfigureAwait(false);

            // Create month
            var monthId = await GetMonthIdentifierAsync(graphClient, date, true).ConfigureAwait(false);
            var prevMonthId = await GetMonthIdentifierAsync(graphClient, date.AddMonths(-1), false).ConfigureAwait(false);
            var nextMonthId = await GetMonthIdentifierAsync(graphClient, date.AddMonths(1), false).ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "PREV", "Month", prevMonthId,
                    "NEXT", "Month", monthId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "PREV", "Month", monthId,
                    "NEXT", "Month", nextMonthId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "CONTAINED_BY", "Year", yearId,
                    "COMPOSED_BY", "Month", monthId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "CONTAINED_BY", "Quarter", quarterId,
                    "SEGMENTED_BY", "Month", monthId)
                .ConfigureAwait(false);


            return null;
        }

        private async Task<string> GetYearIdentifierAsync(IGraphClient graphClient, DateTimeOffset date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = graphClient
                    .Cypher
                    .Merge("a:Year { year: {year} }")
                    .WithParam("year", date.Year)
                    .OnCreate()
                    .Set("id = {id}, year = {year}")
                    .WithParams(
                        new
                        {
                            id = Guid.NewGuid().ToString(),
                            date.Year
                        })
                    .Return<string>("a.id");
                var results = await query.ResultsAsync.ConfigureAwait(false);
                return results.FirstOrDefault();
            }
            else
            {
                var query = graphClient
                    .Cypher
                    .Match("a:Year { year: {year} }")
                    .WithParam("year", date.Year)
                    .Return<string>("a.id");
                var results = await query.ResultsAsync.ConfigureAwait(false);
                return results.FirstOrDefault();
            }
        }

        private async Task<string> GetQuarterIdentifierAsync(IGraphClient graphClient, CalendarQuarter date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = graphClient
                    .Cypher
                    .Merge("a:Quarter { quarter: {quarter}, year: {year} }")
                    .WithParams(
                        new
                        {
                            quarter = date.Quarter,
                            year = date.Year
                        })
                    .OnCreate()
                    .Set("id = {id}, quarter: {quarter}, year = {year}")
                    .WithParams(
                        new
                        {
                            id = Guid.NewGuid().ToString(),
                            quarter = date.Quarter,
                            year = date.Year
                        })
                    .Return<string>("a.id");
                var results = await query.ResultsAsync.ConfigureAwait(false);
                return results.FirstOrDefault();
            }
            else
            {
                var query = graphClient
                    .Cypher
                    .Match("a:Quarter { month: {month}, year: {year} }")
                    .WithParams(
                        new
                        {
                            quarter = date.Quarter,
                            year = date.Year
                        })
                    .Return<string>("a.id");
                var results = await query.ResultsAsync.ConfigureAwait(false);
                return results.FirstOrDefault();
            }
        }

        private async Task<string> GetMonthIdentifierAsync(IGraphClient graphClient, DateTimeOffset date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = graphClient
                    .Cypher
                    .Merge("a:Month { month: {month}, year: {year} }")
                    .WithParams(
                        new
                        {
                            month = date.Month,
                            year = date.Year
                        })
                    .OnCreate()
                    .Set("id = {id}, month: {month}, year = {year}")
                    .WithParams(
                        new
                        {
                            id = Guid.NewGuid().ToString(),
                            month = date.Month,
                            year = date.Year
                        })
                    .Return<string>("a.id");
                var results = await query.ResultsAsync.ConfigureAwait(false);
                return results.FirstOrDefault();
            }
            else
            {
                var query = graphClient
                    .Cypher
                    .Match("a:Month { month: {month}, year: {year} }")
                    .WithParams(
                        new
                        {
                            month = date.Month,
                            year = date.Year
                        })
                    .Return<string>("a.id");
                var results = await query.ResultsAsync.ConfigureAwait(false);
                return results.FirstOrDefault();
            }
        }

        private Task CreateLinkIfNotExistsAsync(
            IGraphClient graphClient,
            string sourceToTargetRelType,
            string sourceLinkType,
            string sourceLinkId,
            string targetToSourceRelType,
            string targetLinkType,
            string targetLinkId)
        {
            // Don't bother if we are missing one of the ends
            if (string.IsNullOrEmpty(sourceLinkId) || string.IsNullOrEmpty(targetLinkId))
            {
                return Task.FromResult(true);
            }

            //MATCH(charlie: Person { name: 'Charlie Sheen' }),(wallStreet: Movie { title: 'Wall Street' })
            //MERGE(charlie) -[r: ACTED_IN]->(wallStreet)
            //    RETURN charlie.name, type(r), wallStreet.title
            var query = graphClient.Cypher
                .Match("from: " + sourceLinkType + " { id: {sourceLinkId} }, to: " + targetLinkType + " { id: {targetLinkId} }")
                .WithParams(
                    new
                    {
                        sourceLinkId,
                        targetLinkId
                    })
                .Merge($"(from)-[n: {sourceToTargetRelType}]->(to)-[p: {targetToSourceRelType}]->(from)");
            return query.ExecuteWithoutResultsAsync();
        }

    }

    public class TimeTreeConfiguration
    {
        public DayOfWeek FirstDayOfWeek { get; set; } = DayOfWeek.Sunday;

        public bool IncludeSecond { get; set; } = false;

        public bool IncludeMinute { get; set; } = false;

        public bool IncludeHour { get; set; } = true;

        public bool IncludeDay { get; set; } = true;

        public bool IncludeWeekNumber { get; set; } = true;

        public bool IncludeMonth { get; set; } = true;

        public bool IncludeQuarter { get; set; } = true;

        public bool IncludeYear { get; set; } = true;
    }
}
