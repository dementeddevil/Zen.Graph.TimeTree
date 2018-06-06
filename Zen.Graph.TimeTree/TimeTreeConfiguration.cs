using System;

namespace Zen.Graph.TimeTree
{
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