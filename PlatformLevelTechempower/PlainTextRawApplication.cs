﻿using System;
using System.IO.Pipelines;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting.Internal;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Protocols.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http;
using Microsoft.AspNetCore.Server.Kestrel.Transport.Abstractions.Internal;
using Utf8Json;

namespace PlatformLevelTechempower
{
    public class PlainTextRawApplication : IConnectionHandler, IServerApplication
    {
        private static AsciiString _crlf = "\r\n";
        private static AsciiString _http11OK = "HTTP/1.1 200 OK\r\n";
        private static AsciiString _headerServer = "Server: Custom";
        private static AsciiString _headerDate = "Date: ";
        private static AsciiString _headerContentLength = "Content-Length: ";
        private static AsciiString _headerContentLengthZero = "Content-Length: 0";
        private static AsciiString _headerContentTypeText = "Content-Type: text/plain";
        private static AsciiString _headerContentTypeJson = "Content-Type: application/json";

        private static readonly DateHeaderValueManager _dateHeaderValueManager = new DateHeaderValueManager();
        private static readonly HttpParser<HttpConnectionContext> _parser = new HttpParser<HttpConnectionContext>();

        private static AsciiString _plainTextBody = "Hello, World!";

        public async Task RunAsync(ITransportFactory transportFactory, IEndPointInformation endPointInformation, ApplicationLifetime lifetime)
        {
            Console.CancelKeyPress += (sender, e) =>
            {
                lifetime.StopApplication();
                lifetime.ApplicationStopped.WaitHandle.WaitOne();
            };

            var transport = transportFactory.Create(endPointInformation, this);

            await transport.BindAsync();

            Console.WriteLine($"Server ({nameof(PlainTextRawApplication)}) listening on http://{endPointInformation.IPEndPoint}");

            lifetime.ApplicationStopping.WaitHandle.WaitOne();

            await transport.UnbindAsync();
            await transport.StopAsync();

            //switch (transportFactory)
            //{
            //    case LibuvTransportFactory _:
            //        Console.WriteLine("ReadCount: {0}, WriteCount: {1}", LibuvTransportFactory.ReadCount, LibuvTransportFactory.WriteCount);
            //        break;
            //    case SocketTransportFactory _:
            //        Console.WriteLine("ReadCount: {0}, WriteCount: {1}", SocketTransportFactory.ReadCount, SocketTransportFactory.WriteCount);
            //        break;
            //}

            //Console.WriteLine("RequestCount: {0}, ParseRequestLineCount: {1}, ParseHeadersCount: {2}", HttpParser<HttpConnectionContext>.RequestCount, HttpParser<HttpConnectionContext>.ParseRequestLineCount, HttpParser<HttpConnectionContext>.ParseHeadersCount);

            lifetime.NotifyStopped();
        }

        public void OnConnection(IFeatureCollection features)
        {
            var transportFeature = features.Get<IConnectionTransportFeature>();
            var connectionIdFeature = features.Get<IConnectionIdFeature>();

            var inputOptions = new PipeOptions(transportFeature.MemoryPool, readerScheduler: null, writerScheduler: transportFeature.InputWriterScheduler);
            var outputOptions = new PipeOptions(transportFeature.MemoryPool, readerScheduler: transportFeature.OutputReaderScheduler);
            var pair = PipeFactory.CreateConnectionPair(inputOptions, outputOptions);

            connectionIdFeature.ConnectionId = Guid.NewGuid().ToString();
            transportFeature.Transport = pair.Transport;
            transportFeature.Application = pair.Application;

            var httpContext = new HttpConnectionContext
            {
                Input = pair.Transport.Input,
                Output = pair.Transport.Output
            };

            _ = httpContext.ExecuteAsync();
        }

        private static class Paths
        {
            public static AsciiString Plaintext = "/plaintext";
            public static AsciiString Json = "/json";
        }

        private class HttpConnectionContext : IHttpHeadersHandler, IHttpRequestLineHandler
        {
            private State _state;

            private HttpMethod _method;
            private byte[] _path;

            // Paths < 256 bytes are copied to here and this is reused per connection
            private byte[] _pathBuffer = new byte[256];
            private int _pathLength;

            public IPipeReader Input { get; set; }

            public IPipeWriter Output { get; set; }

            public async Task ExecuteAsync()
            {
                try
                {
                    while (true)
                    {
                        var result = await Input.ReadAsync();
                        var inputBuffer = result.Buffer;
                        var consumed = inputBuffer.Start;
                        var examined = inputBuffer.End;

                        try
                        {
                            if (inputBuffer.IsEmpty && result.IsCompleted)
                            {
                                break;
                            }

                            ParseHttpRequest(inputBuffer, out consumed, out examined);

                            if (_state != State.Body && result.IsCompleted)
                            {
                                // Bad request
                                break;
                            }

                            if (_state == State.Body)
                            {
                                var outputBuffer = Output.Alloc();

                                if (_method == HttpMethod.Get)
                                {
                                    HandleRequest(ref outputBuffer);
                                }
                                else
                                {
                                    Default(ref outputBuffer);
                                }

                                await outputBuffer.FlushAsync();

                                _path = null;

                                _state = State.StartLine;
                            }
                        }
                        finally
                        {
                            Input.Advance(consumed, examined);
                        }
                    }

                    Input.Complete();
                }
                catch (Exception ex)
                {
                    Input.Complete(ex);
                }
                finally
                {
                    Output.Complete();
                }
            }

            private void HandleRequest(ref WritableBuffer outputBuffer)
            {
                Span<byte> path;

                if (_path != null)
                {
                    path = _path;
                }
                else
                {
                    path = new Span<byte>(_pathBuffer, 0, _pathLength);
                }

                if (path.StartsWith(Paths.Plaintext))
                {
                    PlainText(ref outputBuffer);
                }
                else if (path.StartsWith(Paths.Json))
                {
                    Json(ref outputBuffer);
                }
                else
                {
                    Default(ref outputBuffer);
                }
            }

            private static void Default(ref WritableBuffer outputBuffer)
            {
                var writer = new WritableBufferWriter(outputBuffer);

                // HTTP 1.1 OK
                writer.Write(_http11OK);

                // Server headers
                writer.Write(_headerServer);

                // Date header
                writer.Write(_dateHeaderValueManager.GetDateHeaderValues().Bytes);
                writer.Write(_crlf);

                // Content-Length 0
                writer.Write(_headerContentLengthZero);
                writer.Write(_crlf);

                // End of headers
                writer.Write(_crlf);
            }

            private static void Json(ref WritableBuffer outputBuffer)
            {
                var writer = new WritableBufferWriter(outputBuffer);

                // HTTP 1.1 OK
                writer.Write(_http11OK);

                // Server headers
                writer.Write(_headerServer);

                // Date header
                writer.Write(_dateHeaderValueManager.GetDateHeaderValues().Bytes);
                writer.Write(_crlf);

                // Content-Type header
                writer.Write(_headerContentTypeJson);
                writer.Write(_crlf);

                var jsonPayload = JsonSerializer.SerializeUnsafe(new { message = "Hello, World!" });

                // Content-Length header
                writer.Write(_headerContentLength);
                Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http.PipelineExtensions.WriteNumeric(ref writer, (ulong)jsonPayload.Count);
                writer.Write(_crlf);

                // End of headers
                writer.Write(_crlf);

                // Body
                writer.Write(jsonPayload.Array, jsonPayload.Offset, jsonPayload.Count);
            }

            private static void PlainText(ref WritableBuffer outputBuffer)
            {
                var writer = new WritableBufferWriter(outputBuffer);
                // HTTP 1.1 OK
                writer.Write(_http11OK);

                // Server headers
                writer.Write(_headerServer);

                // Date header
                writer.Write(_dateHeaderValueManager.GetDateHeaderValues().Bytes);
                writer.Write(_crlf);

                // Content-Type header
                writer.Write(_headerContentTypeText);
                writer.Write(_crlf);

                // Content-Length header
                writer.Write(_headerContentLength);
                Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http.PipelineExtensions.WriteNumeric(ref writer, (ulong)_plainTextBody.Length);
                writer.Write(_crlf);

                // End of headers
                writer.Write(_crlf);

                // Body
                writer.Write(_plainTextBody);
            }

            private void ParseHttpRequest(ReadableBuffer inputBuffer, out ReadCursor consumed, out ReadCursor examined)
            {
                consumed = inputBuffer.Start;
                examined = inputBuffer.End;

                if (_state == State.StartLine)
                {
                    if (_parser.ParseRequestLine(this, inputBuffer, out consumed, out examined))
                    {
                        _state = State.Headers;
                        inputBuffer = inputBuffer.Slice(consumed);
                    }
                }

                if (_state == State.Headers)
                {
                    if (_parser.ParseHeaders(this, inputBuffer, out consumed, out examined, out int consumedBytes))
                    {
                        _state = State.Body;
                    }
                }
            }

            public void OnStartLine(HttpMethod method, HttpVersion version, Span<byte> target, Span<byte> path, Span<byte> query, Span<byte> customMethod, bool pathEncoded)
            {
                _method = method;

                if (path.TryCopyTo(_pathBuffer))
                {
                    _pathLength = path.Length;
                }
                else // path > 256
                {
                    _path = path.ToArray();
                }
            }

            public void OnHeader(Span<byte> name, Span<byte> value)
            {
            }

            private enum State
            {
                StartLine,
                Headers,
                Body
            }
        }
    }
}
