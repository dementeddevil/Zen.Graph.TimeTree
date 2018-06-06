using System;
using System.Linq;
using System.Threading.Tasks;
using Neo4jClient;
using Zen.Graph.TimeTree.Helpers;

namespace Zen.Graph.TimeTree
{
    public class TimeTreeService : ITimeTreeService
    {
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
                    "CHILD_MONTH", "Year", yearId,
                    "PARENT_YEAR", "Month", monthId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "CHILD_MONTH", "Quarter", quarterId,
                    "PARENT_QUARTER", "Month", monthId)
                .ConfigureAwait(false);

            // Create week
            var week = new CalendarWeek(date, _configuration.FirstDayOfWeek);
            var weekId = await GetWeekIdentifierAsync(graphClient, week, true).ConfigureAwait(false);
            var prevWeekId = await GetWeekIdentifierAsync(graphClient, week.Previous, false).ConfigureAwait(false);
            var nextWeekId = await GetWeekIdentifierAsync(graphClient, week.Next, false).ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "PREV", "Week", prevWeekId,
                    "NEXT", "Week", weekId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "PREV", "Week", weekId,
                    "NEXT", "Week", nextWeekId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "CHILD_WEEK", "Year", yearId,
                    "PARENT_YEAR", "Week", weekId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "CHILD_WEEK", "Quarter", quarterId,
                    "PARENT_QUARTER", "Week", weekId)
                .ConfigureAwait(false);

            // Create day
            var dayId = await GetDayIdentifierAsync(graphClient, date, true).ConfigureAwait(false);
            var prevDayId = await GetDayIdentifierAsync(graphClient, date.AddDays(-1), false).ConfigureAwait(false);
            var nextDayId = await GetDayIdentifierAsync(graphClient, date.AddDays(1), false).ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "PREV", "Day", prevDayId,
                    "NEXT", "Day", weekId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "PREV", "Day", weekId,
                    "NEXT", "Day", nextDayId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "CHILD_DAY", "Month", quarterId,
                    "PARENT_MONTH", "Day", dayId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    graphClient,
                    "CHILD_DAY", "Week", weekId,
                    "PARENT_WEEK", "Day", dayId)
                .ConfigureAwait(false);

            return dayId;
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

        private async Task<string> GetWeekIdentifierAsync(IGraphClient graphClient, CalendarWeek date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = graphClient
                    .Cypher
                    .Merge("a:Week { week: {week}, year: {year} }")
                    .WithParams(
                        new
                        {
                            week = date.WeekNumber,
                            year = date.Date.Year
                        })
                    .OnCreate()
                    .Set("id = {id}, week: {week}, year = {year}")
                    .WithParams(
                        new
                        {
                            id = Guid.NewGuid().ToString(),
                            week = date.WeekNumber,
                            year = date.Date.Year
                        })
                    .Return<string>("a.id");
                var results = await query.ResultsAsync.ConfigureAwait(false);
                return results.FirstOrDefault();
            }
            else
            {
                var query = graphClient
                    .Cypher
                    .Match("a:Week { week: {week}, year: {year} }")
                    .WithParams(
                        new
                        {
                            week = date.WeekNumber,
                            year = date.Date.Year
                        })
                    .Return<string>("a.id");
                var results = await query.ResultsAsync.ConfigureAwait(false);
                return results.FirstOrDefault();
            }
        }

        private async Task<string> GetDayIdentifierAsync(IGraphClient graphClient, DateTimeOffset date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = graphClient
                    .Cypher
                    .Merge("a:Day { day: {day}, month: {month}, year: {year} }")
                    .WithParams(
                        new
                        {
                            day = date.Day,
                            month = date.Month,
                            year = date.Year
                        })
                    .OnCreate()
                    .Set("id = {id}, day: {day}, month: {month}, year = {year}")
                    .WithParams(
                        new
                        {
                            id = Guid.NewGuid().ToString(),
                            day = date.Day,
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
                    .Match("a:Day { day: {day}, month: {month}, year: {year} }")
                    .WithParams(
                        new
                        {
                            day = date.Day,
                            month = date.Month,
                            year = date.Year
                        })
                    .Return<string>("a.id");
                var results = await query.ResultsAsync.ConfigureAwait(false);
                return results.FirstOrDefault();
            }
        }

        private async Task<string> GetHourIdentifierAsync(IGraphClient graphClient, DateTimeOffset date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = graphClient
                    .Cypher
                    .Merge("a:Hour { hour: {hour}, day: {day}, month: {month}, year: {year} }")
                    .WithParams(
                        new
                        {
                            hour = date.Hour,
                            day = date.Day,
                            month = date.Month,
                            year = date.Year
                        })
                    .OnCreate()
                    .Set("id = {id}, hour: {hour}, day: {day}, month: {month}, year = {year}")
                    .WithParams(
                        new
                        {
                            id = Guid.NewGuid().ToString(),
                            hour = date.Hour,
                            day = date.Day,
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
                    .Match("a:Hour { hour: {hour}, day: {day}, month: {month}, year: {year} }")
                    .WithParams(
                        new
                        {
                            hour = date.Hour,
                            day = date.Day,
                            month = date.Month,
                            year = date.Year
                        })
                    .Return<string>("a.id");
                var results = await query.ResultsAsync.ConfigureAwait(false);
                return results.FirstOrDefault();
            }
        }

        private async Task<string> GetMinuteIdentifierAsync(IGraphClient graphClient, DateTimeOffset date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = graphClient
                    .Cypher
                    .Merge("a:Minute { minute: {minute}, hour: {hour}, day: {day}, month: {month}, year: {year} }")
                    .WithParams(
                        new
                        {
                            minute = date.Minute,
                            hour = date.Hour,
                            day = date.Day,
                            month = date.Month,
                            year = date.Year
                        })
                    .OnCreate()
                    .Set("id = {id}, minute: {minute}, hour: {hour}, day: {day}, month: {month}, year = {year}")
                    .WithParams(
                        new
                        {
                            id = Guid.NewGuid().ToString(),
                            minute = date.Minute,
                            hour = date.Hour,
                            day = date.Day,
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
                    .Match("a:Minute { minute: {minute}, hour: {hour}, day: {day}, month: {month}, year: {year} }")
                    .WithParams(
                        new
                        {
                            minute = date.Minute,
                            hour = date.Hour,
                            day = date.Day,
                            month = date.Month,
                            year = date.Year
                        })
                    .Return<string>("a.id");
                var results = await query.ResultsAsync.ConfigureAwait(false);
                return results.FirstOrDefault();
            }
        }

        private async Task<string> GetSecondIdentifierAsync(IGraphClient graphClient, DateTimeOffset date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = graphClient
                    .Cypher
                    .Merge("a:Second { second: {second}, minute: {minute}, hour: {hour}, day: {day}, month: {month}, year: {year} }")
                    .WithParams(
                        new
                        {
                            second = date.Second,
                            minute = date.Minute,
                            hour = date.Hour,
                            day = date.Day,
                            month = date.Month,
                            year = date.Year
                        })
                    .OnCreate()
                    .Set("id = {id}, second: {second}, minute: {minute}, hour: {hour}, day: {day}, month: {month}, year = {year}")
                    .WithParams(
                        new
                        {
                            id = Guid.NewGuid().ToString(),
                            second = date.Second,
                            minute = date.Minute,
                            hour = date.Hour,
                            day = date.Day,
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
                    .Match("a:Second { second: {second}, minute: {minute}, hour: {hour}, day: {day}, month: {month}, year: {year} }")
                    .WithParams(
                        new
                        {
                            second = date.Second,
                            minute = date.Minute,
                            hour = date.Hour,
                            day = date.Day,
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
}