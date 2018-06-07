﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Neo4jClient;
using Zen.Graph.TimeTree.Helpers;

namespace Zen.Graph.TimeTree
{
    public class TimeTreeService : ITimeTreeService
    {
        private readonly IGraphClient _graphClient;
        private readonly TimeTreeConfiguration _configuration;

        public TimeTreeService(IGraphClient graphClient, TimeTreeConfiguration configuration)
        {
            _graphClient = graphClient;
            _configuration = configuration;
        }

        public async Task<string> Get(DateTimeOffset date)
        {
            // Create year (link if possible)
            var yearId = await GetYearIdentifierAsync(date, true).ConfigureAwait(false);
            var prevYearId = await GetYearIdentifierAsync(date.AddYears(-1), false).ConfigureAwait(false);
            var nextYearId = await GetYearIdentifierAsync(date.AddYears(1), false).ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Year", prevYearId,
                    "NEXT", "Year", yearId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Year", yearId,
                    "NEXT", "Year", nextYearId)
                .ConfigureAwait(false);

            // Create quarter
            var quarter = new CalendarQuarter(date);
            var quarterId = await GetQuarterIdentifierAsync(quarter, true).ConfigureAwait(false);
            var prevQuarterId = await GetQuarterIdentifierAsync(quarter.Previous, false).ConfigureAwait(false);
            var nextQuarterId = await GetQuarterIdentifierAsync(quarter.Next, false).ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Quarter", prevQuarterId,
                    "NEXT", "Quarter", quarterId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Quarter", quarterId,
                    "NEXT", "Quarter", nextQuarterId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "CONTAINED_BY", "Year", yearId,
                    "SEGMENTED_BY", "Quarter", quarterId)
                .ConfigureAwait(false);

            // Create month
            var monthId = await GetMonthIdentifierAsync(date, true).ConfigureAwait(false);
            var prevMonthId = await GetMonthIdentifierAsync(date.AddMonths(-1), false).ConfigureAwait(false);
            var nextMonthId = await GetMonthIdentifierAsync(date.AddMonths(1), false).ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Month", prevMonthId,
                    "NEXT", "Month", monthId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Month", monthId,
                    "NEXT", "Month", nextMonthId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "CHILD_MONTH", "Year", yearId,
                    "PARENT_YEAR", "Month", monthId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "CHILD_MONTH", "Quarter", quarterId,
                    "PARENT_QUARTER", "Month", monthId)
                .ConfigureAwait(false);

            // Create week
            var week = new CalendarWeek(date, _configuration.FirstDayOfWeek);
            var weekId = await GetWeekIdentifierAsync(week, true).ConfigureAwait(false);
            var prevWeekId = await GetWeekIdentifierAsync(week.Previous, false).ConfigureAwait(false);
            var nextWeekId = await GetWeekIdentifierAsync(week.Next, false).ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Week", prevWeekId,
                    "NEXT", "Week", weekId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Week", weekId,
                    "NEXT", "Week", nextWeekId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "CHILD_WEEK", "Year", yearId,
                    "PARENT_YEAR", "Week", weekId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "CHILD_WEEK", "Quarter", quarterId,
                    "PARENT_QUARTER", "Week", weekId)
                .ConfigureAwait(false);

            // Create day
            var dayId = await GetDayIdentifierAsync(date, true).ConfigureAwait(false);
            var prevDayId = await GetDayIdentifierAsync(date.AddDays(-1), false).ConfigureAwait(false);
            var nextDayId = await GetDayIdentifierAsync(date.AddDays(1), false).ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Day", prevDayId,
                    "NEXT", "Day", weekId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Day", weekId,
                    "NEXT", "Day", nextDayId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "CHILD_DAY", "Month", quarterId,
                    "PARENT_MONTH", "Day", dayId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "CHILD_DAY", "Week", weekId,
                    "PARENT_WEEK", "Day", dayId)
                .ConfigureAwait(false);

            // Create hour
            var hourId = await GetHourIdentifierAsync(date, true).ConfigureAwait(false);
            var prevHourId = await GetHourIdentifierAsync(date.AddHours(-1), false).ConfigureAwait(false);
            var nextHourId = await GetHourIdentifierAsync(date.AddHours(1), false).ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Hour", prevHourId,
                    "NEXT", "Hour", hourId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Hour", hourId,
                    "NEXT", "Hour", nextHourId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "CHILD_HOUR", "Day", dayId,
                    "PARENT_DAY", "Hour", hourId)
                .ConfigureAwait(false);

            // Create minute
            var minuteId = await GetMinuteIdentifierAsync(date, true).ConfigureAwait(false);
            var prevMinuteId = await GetMinuteIdentifierAsync(date.AddMinutes(-1), false).ConfigureAwait(false);
            var nextMinuteId = await GetMinuteIdentifierAsync(date.AddMinutes(1), false).ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Minute", prevMinuteId,
                    "NEXT", "Minute", minuteId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Minute", minuteId,
                    "NEXT", "Minute", nextMinuteId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "CHILD_HOUR", "Minute", hourId,
                    "PARENT_DAY", "Hour", minuteId)
                .ConfigureAwait(false);

            // Create second
            var secondId = await GetSecondIdentifierAsync(date, true).ConfigureAwait(false);
            var prevSecondId = await GetSecondIdentifierAsync(date.AddSeconds(-1), false).ConfigureAwait(false);
            var nextSecondId = await GetSecondIdentifierAsync(date.AddSeconds(1), false).ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Second", prevSecondId,
                    "NEXT", "Second", secondId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "PREV", "Second", secondId,
                    "NEXT", "Second", nextSecondId)
                .ConfigureAwait(false);
            await CreateLinkIfNotExistsAsync(
                    "CHILD_HOUR", "Second", minuteId,
                    "PARENT_DAY", "Minute", secondId)
                .ConfigureAwait(false);

            return secondId;
        }

        private async Task<string> GetYearIdentifierAsync(DateTimeOffset date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = _graphClient
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
                var query = _graphClient
                    .Cypher
                    .Match("a:Year { year: {year} }")
                    .WithParam("year", date.Year)
                    .Return<string>("a.id");
                var results = await query.ResultsAsync.ConfigureAwait(false);
                return results.FirstOrDefault();
            }
        }

        private async Task<string> GetQuarterIdentifierAsync(CalendarQuarter date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = _graphClient
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
                var query = _graphClient
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

        private async Task<string> GetMonthIdentifierAsync(DateTimeOffset date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = _graphClient
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
                var query = _graphClient
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

        private async Task<string> GetWeekIdentifierAsync(CalendarWeek date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = _graphClient
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
                var query = _graphClient
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

        private async Task<string> GetDayIdentifierAsync(DateTimeOffset date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = _graphClient
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
                var query = _graphClient
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

        private async Task<string> GetHourIdentifierAsync(DateTimeOffset date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = _graphClient
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
                var query = _graphClient
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

        private async Task<string> GetMinuteIdentifierAsync(DateTimeOffset date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = _graphClient
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
                var query = _graphClient
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

        private async Task<string> GetSecondIdentifierAsync(DateTimeOffset date, bool createIfNotExists)
        {
            if (createIfNotExists)
            {
                var query = _graphClient
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
                var query = _graphClient
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
            var query = _graphClient.Cypher
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