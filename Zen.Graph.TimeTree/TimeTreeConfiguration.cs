using System;

namespace Zen.Graph.TimeTree
{
    public class TimeTreeConfiguration
    {
        public DayOfWeek FirstDayOfWeek { get; set; } = DayOfWeek.Sunday;

        public TimeTreeSpecificity Specificity { get; set; } = TimeTreeSpecificity.Minutes;
    }

    public enum TimeTreeSpecificity
    {
        Years,
        Quarters,
        Months,
        Weeks,
        Days,
        Hours,
        Minutes,
        Secounds
    }
}