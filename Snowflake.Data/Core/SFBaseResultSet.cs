﻿/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{

    abstract class SFBaseResultSet
    {
        internal SFStatement sfStatement;

        internal SFResultSetMetaData sfResultSetMetaData;

        internal int columnCount;

        internal bool isClosed;

        internal abstract bool Next();

        internal abstract Task<bool> NextAsync();

        protected abstract UTF8Buffer getObjectInternal(int columnIndex);

        protected Stopwatch parseTimer = new Stopwatch();
        protected Stopwatch waitTimer = new Stopwatch();
        protected Stopwatch elapsedTimer = Stopwatch.StartNew();

        protected SFBaseResultSet()
        {
        }

        internal T GetValue<T>(int columnIndex)
        {
            parseTimer.Start();
            UTF8Buffer val = getObjectInternal(columnIndex);
            var types = sfResultSetMetaData.GetTypesByIndex(columnIndex);
            T result = (T)SFDataConverter.ConvertToCSharpVal(val, types.Item1, typeof(T));
            parseTimer.Stop();
            return result;
        }

        internal string GetString(int columnIndex)
        {
            parseTimer.Start();
            string result;
            var type = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            switch (type)
            {
                case SFDataType.DATE:
                    var val = GetValue(columnIndex);
                    if (val == DBNull.Value)
                        return null;
                    result =  SFDataConverter.toDateString((DateTime)val, 
                        sfResultSetMetaData.dateOutputFormat);
                    break;
                //TODO: Implement SqlFormat for timestamp type, aka parsing format specified by user and format the value
                default:
                    result = getObjectInternal(columnIndex).SafeToString();
                    break;
            }
            parseTimer.Stop();
            return result;
        }

        internal object GetValue(int columnIndex)
        {
            parseTimer.Start();
            UTF8Buffer val = getObjectInternal(columnIndex);
            var types = sfResultSetMetaData.GetTypesByIndex(columnIndex);
            object result = SFDataConverter.ConvertToCSharpVal(val, types.Item1, types.Item2);
            parseTimer.Stop();
            return result;
        }
        
        internal void close()
        {
            isClosed = true;
        }
        
    }
}
