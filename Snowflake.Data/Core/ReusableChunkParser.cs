﻿/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.IO;
using Newtonsoft.Json;

namespace Snowflake.Data.Core
{
    using Snowflake.Data.Client;

    public class FastStreamWrapper
    {
        Stream wrappedStream;
        byte[] buffer = new byte[8192];
        int count = 0;
        int next = 0;

        public FastStreamWrapper(Stream s)
        {
            wrappedStream = s;
        }

        // Small method to encourage inlining
        public int ReadByte()
        {
            // fast path first
            if (next < count)
                return buffer[next++];
            else
                return ReadByteSlow();

        }

        private int ReadByteSlow()
        {
            // fast path first
            if (next < count)
                return buffer[next++];

            if (count >= 0)
            {
                next = 0;
                count = wrappedStream.ReadAsync(buffer, 0, buffer.Length).GetAwaiter().GetResult();
            }

            if (count <= 0)
            {
                count = -1;
                return -1;
            }

            return buffer[next++];
        }
    }

    public class ReusableChunkParser : IChunkParser
    {
        // Very fast parser, only supports strings and nulls
        // Never generates parsing errors

        private readonly Stream stream;

        internal ReusableChunkParser(Stream stream)
        {
            this.stream = stream;
        }

        public void ParseChunk(IResultChunk chunk)
        {
            SFReusableChunk rc = (SFReusableChunk)chunk;

            bool inString = false;
            int c;
            var input = new FastStreamWrapper(stream);
            MemoryStream ms = new MemoryStream();
            while ((c = input.ReadByte()) >= 0)
            {
                if (!inString)
                {
                    // n means null
                    // " quote means begin string
                    // all else are ignored
                    if (c == '"')
                    {
                        inString = true;
                    }
                    else if (c == 'n')
                    {
                        rc.AddCell(null, 0);
                    }
                    // ignore anything else
                }
                else
                {
                    // Inside a string, look for end string
                    // Anything else is saved in the buffer
                    if (c == '"')
                    {
                        rc.AddCell(ms.GetBuffer(), (int)ms.Length);
                        ms.SetLength(0);
                        inString = false;
                    }
                    else if (c == '\\')
                    {
                        // Process next character
                        c = input.ReadByte();
                        switch (c)
                        {
                            case 'n':
                                c = 10;
                                break;
                            case 'r':
                                c = 13;
                                break;
                            case 'b':
                                c = 8;
                                break;
                        }
                        ms.WriteByte((byte)c);
                    }
                    else
                    {
                        ms.WriteByte((byte)c);
                    }
                }
            }
        }
    }

    class ReusableChunkParserOld : IChunkParser
    {
        private readonly Stream stream;

        internal ReusableChunkParserOld(Stream stream)
        {
            this.stream = stream;
        }

        public void ParseChunk(IResultChunk chunk)
        {
            SFReusableChunk rc = (SFReusableChunk)chunk;
            // parse results row by row
            using (StreamReader sr = new StreamReader(stream))
            using (JsonTextReader jr = new JsonTextReader(sr) { DateParseHandling = DateParseHandling.None })
            {
                while (jr.Read())
                {
                    switch (jr.TokenType)
                    {
                        case JsonToken.StartArray:
                        case JsonToken.None:
                        case JsonToken.EndArray:
                            break;

                        case JsonToken.Null:
                            rc.AddCell(null);
                            break;

                        case JsonToken.String:
                            rc.AddCell((string)jr.Value);
                            break;

                        default:
                            throw new SnowflakeDbException(SFError.INTERNAL_ERROR, $"Unexpected token type: {jr.TokenType}");
                    }
                }
            }
        }
    }
}
