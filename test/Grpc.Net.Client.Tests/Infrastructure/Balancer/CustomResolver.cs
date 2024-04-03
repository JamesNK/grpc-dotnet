#if SUPPORT_LOAD_BALANCING
using Grpc.Net.Client.Balancer;
using Microsoft.Extensions.Logging;

namespace Grpc.Tests.Shared;

/// <summary>
/// Test Resolver but a bit similar to really used in our project
/// </summary>
internal class CustomResolver(ILoggerFactory loggerFactory) : PollingResolver(loggerFactory)
{
    private const int AddressesUpdatePeriodMs = 500;

    private static bool ShouldRenew = true;

    // address lists should be different
    private readonly string[][] _addresses = [
        [ "test_addr_01", "test_addr_02" ],
        [ "test_addr_03", "test_addr_04" ],
    ];

    private volatile int _updateAddressesIndex;
    private Timer? _timer;
    private readonly object _lock = new ();
    private readonly object _lock2 = new ();

    private ResolverResult _result = ResolverResult.ForResult([]);

    protected override void OnStarted()
    {
        // periodically renew address lists
        _timer = new Timer(
            NotifyListenerAboutResolverResult,
            null,
            AddressesUpdatePeriodMs,
            AddressesUpdatePeriodMs);

        NotifyListenerAboutResolverResult();
    }

    public static void StopRenew()
    {
        ShouldRenew = false;
    }

    public static  void RestartRenew()
    {
        ShouldRenew = true;
    }

    protected override async Task ResolveAsync(CancellationToken cancellationToken)
    {
        await Task.Yield(); // fix used to avoid deadlock

        lock (_lock)
        {
            Listener(_result);
        }
    }

    private void NotifyListenerAboutResolverResult(object? state = null)
    {
        if (!ShouldRenew)
        {
            return;
        }

        lock (_lock)
        {
            UpdateResolverState();
            Listener(_result);
        }
    }

    // fill `_result` field with addresses we will use during the next `Listener()` call
    private void UpdateResolverState()
    {
        var addresses = _addresses[_updateAddressesIndex];

        var balancerAddresses = addresses
            .Select(host => new BalancerAddress(host, 4242))
            .ToArray();

        _result = ResolverResult.ForResult(balancerAddresses);

        if (_result.Addresses?.Count == 0)
        {
            return;
        }

        // choose next addresses list (or the first one if the end of the list is reached)
        lock (_lock2)
        {
            Interlocked.Increment(ref _updateAddressesIndex);
            Interlocked.CompareExchange(ref _updateAddressesIndex, 0, _addresses.Length);
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (!disposing)
        {
            return;
        }

        _timer?.Dispose();
    }
}
#endif
