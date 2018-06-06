using System;

namespace Zen.Graph.TimeTree.Helpers
{
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
}
