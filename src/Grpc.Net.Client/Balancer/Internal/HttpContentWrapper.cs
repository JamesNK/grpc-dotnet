#region Copyright notice and license

// Copyright 2019 The gRPC Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#endregion

using System.Net;
using Grpc.Shared;

namespace Grpc.Net.Client.Balancer.Internal;

internal sealed class HttpContentWrapper : HttpContent
{
    private readonly HttpContent _inner;
    private readonly CancellationToken _cancellationToken;
    private readonly Action _disposeAction;
    private readonly MemoryStream _stream;
    private Stream? _innerStream;
    private bool _disposed;

    public HttpContentWrapper(HttpContent inner, CancellationToken cancellationToken, Action disposeAction)
    {
        _inner = inner;
        _cancellationToken = cancellationToken;
        _disposeAction = disposeAction;
        _stream = new MemoryStream();

        foreach (var kvp in inner.Headers)
        {
            Headers.TryAddWithoutValidation(kvp.Key, kvp.Value.ToArray());
        }
    }

    protected override async Task<Stream> CreateContentReadStreamAsync()
    {
        if (_innerStream is null)
        {
            _innerStream = await _inner.ReadAsStreamAsync().ConfigureAwait(false);
            _ = DrainStream();
        }

        return _stream;
    }

    private async Task DrainStream()
    {
        CompatibilityHelpers.Assert(_innerStream is not null);

        try
        {
#if NETSTANDARD2_0
            await _innerStream.CopyToAsync(_stream, 1024 * 8, _cancellationToken).ConfigureAwait(false);
#else
            await _innerStream.CopyToAsync(_stream, _cancellationToken).ConfigureAwait(false);
#endif
        }
        catch
        {
            // Catch error to prevent it being thrown from unobserved task.
        }
        finally
        {
            Dispose(true);
        }
    }

    protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context)
    {
        throw new NotImplementedException();
    }

    protected override bool TryComputeLength(out long length)
    {
        length = 0;
        return false;
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        if (disposing && !_disposed)
        {
            _disposeAction();
            _inner.Dispose();
            _stream.Dispose();
            _disposed = true;
        }
    }
}
