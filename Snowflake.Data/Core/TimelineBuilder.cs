using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Core
{
    public class TimelineBuilder
    {
        DateTime firstTime;
        string value;
        public double ms_per_char;

        public TimelineBuilder(DateTime firstTime, DateTime lastTime, int CharCount)
        {
            this.firstTime = firstTime;
            ms_per_char = (lastTime - firstTime).TotalMilliseconds / CharCount;
            value = string.Empty;
        }

        public void AddSegment(char symbol, DateTime targetTime, bool MustBeVisible)
        {
            // If MustBeVisible is true, we always add at least one character unless we are already beyond the desired targetTime
            double current_ms = value.Length * ms_per_char;
            double target_ms = (targetTime - firstTime).TotalMilliseconds;
            if (target_ms < current_ms)
                return;
            int n = (int)((target_ms - current_ms) / ms_per_char);
            if (n == 0 && MustBeVisible)
                n = 1;
            value += new string(symbol, n);
        }

        public override string ToString()
        {
            return value;
        }

        // Prepare for generating a new timeline
        public void Reset()
        {
            value = string.Empty;
        }
    }
}
