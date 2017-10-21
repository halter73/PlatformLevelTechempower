using System;
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
    public class PlainTextRawWithHeadersApplication : IConnectionHandler, IServerApplication
    {
        private static AsciiString _bytesHttpVersion11 = "HTTP/1.1 ";
        private static AsciiString _headerServer = "\r\nServer: Custom";
        private static AsciiString _plainTextBody = "Hello, World!";
        private static AsciiString _bytesEndHeaders = "\r\n\r\n";

        private static readonly DateHeaderValueManager _dateHeaderValueManager = new DateHeaderValueManager();
        private static readonly HttpParser<HttpConnectionContext> _parser = new HttpParser<HttpConnectionContext>();

        public async Task RunAsync(ITransportFactory transportFactory, IEndPointInformation endPointInformation, ApplicationLifetime lifetime)
        {
            Console.CancelKeyPress += (sender, e) => lifetime.StopApplication();

            var transport = transportFactory.Create(endPointInformation, this);

            await transport.BindAsync();

            Console.WriteLine($"Server ({nameof(PlainTextRawWithHeadersApplication)}) listening on http://{endPointInformation.IPEndPoint}");

            lifetime.ApplicationStopping.WaitHandle.WaitOne();

            await transport.UnbindAsync();
            await transport.StopAsync();
        }

        public void OnConnection(IFeatureCollection features)
        {
            var transportFeature = features.Get<IConnectionTransportFeature>();
            var connectionIdFeature = features.Get<IConnectionIdFeature>();

            var inputOptions = new PipeOptions { WriterScheduler = transportFeature.InputWriterScheduler };
            var outputOptions = new PipeOptions { ReaderScheduler = transportFeature.OutputReaderScheduler };
            var pair = transportFeature.PipeFactory.CreateConnectionPair(inputOptions, outputOptions);

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

            private HttpResponseHeaders ResponseHeaders = new HttpResponseHeaders();

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
                                ResponseHeaders.Reset();

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

            private void Default(ref WritableBuffer outputBuffer)
            {
                var writer = new WritableBufferWriter(outputBuffer);

                // HTTP 1.1 OK
                writer.Write(_bytesHttpVersion11);
                writer.Write(ReasonPhrases.ToStatusBytes(200, reasonPhrase: null));

                // Headers
                var values = _dateHeaderValueManager.GetDateHeaderValues();
                ResponseHeaders.SetRawDate(values.String, values.Bytes);
                ResponseHeaders.SetRawServer("Custom", _headerServer);
                ResponseHeaders.ContentLength = 0;

                // Write headers
                ResponseHeaders.CopyTo(ref writer);
                writer.Write(_bytesEndHeaders);
            }

            private void Json(ref WritableBuffer outputBuffer)
            {
                var writer = new WritableBufferWriter(outputBuffer);

                // HTTP 1.1 OK
                writer.Write(_bytesHttpVersion11);
                writer.Write(ReasonPhrases.ToStatusBytes(200, reasonPhrase: null));

                // Headers
                var values = _dateHeaderValueManager.GetDateHeaderValues();
                ResponseHeaders.SetRawDate(values.String, values.Bytes);
                ResponseHeaders.SetRawServer("Custom", _headerServer);
                ResponseHeaders.HeaderContentType = "application/json";
                var jsonPayload = JsonSerializer.SerializeUnsafe(new { message = "Hello, World!" });
                ResponseHeaders.ContentLength = jsonPayload.Count;

                // Write headers
                ResponseHeaders.CopyTo(ref writer);
                writer.Write(_bytesEndHeaders);

                // Body
                writer.Write(jsonPayload.Array, jsonPayload.Offset, jsonPayload.Count);
            }

            private void PlainText(ref WritableBuffer outputBuffer)
            {
                var writer = new WritableBufferWriter(outputBuffer);

                // HTTP 1.1 OK
                writer.Write(_bytesHttpVersion11);
                writer.Write(ReasonPhrases.ToStatusBytes(200, reasonPhrase: null));

                // Headers
                var values = _dateHeaderValueManager.GetDateHeaderValues();
                ResponseHeaders.SetRawDate(values.String, values.Bytes);
                ResponseHeaders.SetRawServer("Custom", _headerServer);
                ResponseHeaders.HeaderContentType = "text/plain";
                ResponseHeaders.ContentLength = _plainTextBody.Length;

                // Write headers
                ResponseHeaders.CopyTo(ref writer);
                writer.Write(_bytesEndHeaders);

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
