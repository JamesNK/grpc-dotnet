using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

#if NET5_0_OR_GREATER
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Grpc.Net.Client.Balancer
{
    public abstract class AddressResolverFactory
    {
        public abstract string Name { get; }
        public abstract AddressResolver Create(Uri address, AddressResolverOptions options);
    }

    public sealed class AddressResolverOptions
    {
    }
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#endif
