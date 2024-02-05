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

using System.Diagnostics;
using Greet;
using Grpc.Net.Client;

using var channel = GrpcChannel.ForAddress("https://localhost:5001");
var client = new Greeter.GreeterClient(channel);

await client.SayHelloAsync(new HelloRequest { Name = "GreeterClient" });

var stopwatch = Stopwatch.StartNew();

var senders = new List<Task>();
var times = new List<double>();

for (int i = 0; i < 100; i++)
{
    senders.Add(Task.Run(async () =>
    {
        var list = new List<double>();
        for (int i = 0; i < 10000; i++)
        {
            var start = Stopwatch.GetTimestamp();
            await client.SayHelloAsync(new HelloRequest { Name = "GreeterClient" });
            var total = Stopwatch.GetTimestamp() - start;
            list.Add(total);
        }
        lock (times)
        {
            times.AddRange(list);
        }
    }));
}

await Task.WhenAll(senders);

var average = times.Sum() / times.Count;

stopwatch.Stop();
Console.WriteLine($"Total time: {stopwatch.Elapsed.TotalSeconds} seconds");
Console.WriteLine($"Average request time: {TimeSpan.FromTicks((long)average).TotalMilliseconds} ms");

Console.WriteLine("Shutting down");
Console.WriteLine("Press any key to exit...");
Console.ReadKey();
