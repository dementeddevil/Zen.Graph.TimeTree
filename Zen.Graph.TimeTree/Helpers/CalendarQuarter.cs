using System;

namespace Zen.Graph.TimeTree.Helpers
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

            while (quarter > 4)
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
}