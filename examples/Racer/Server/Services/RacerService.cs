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

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Race;

namespace Server
{
    public class RacerService : Racer.RacerBase
    {
        private ILogger<RacerService> _logger;

        public RacerService(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<RacerService>();
        }

        public override async Task ReadySetGo(IAsyncStreamReader<RaceMessage> requestStream, IServerStreamWriter<RaceMessage> responseStream, ServerCallContext context)
        {
            var raceDuration = TimeSpan.Parse(context.RequestHeaders.Single(h => h.Key == "race-duration").Value);

            // Read incoming messages in a background task
            RaceMessage? lastMessageReceived = null;
            var readTask = Task.Run(async () =>
            {
                _logger.LogInformation($"Started reading from client");

                await foreach (var message in requestStream.ReadAllAsync())
                {
                    lastMessageReceived = message;
                }

                _logger.LogInformation($"Finished reading from client");
            });

            for (int i = 0; i < 2; i++)
            {
                _logger.LogInformation($"Iteration {i} - Start");

                // Write outgoing messages until timer is complete
                var sw = Stopwatch.StartNew();
                var sent = 0;
                while (sw.Elapsed < raceDuration)
                {
                    await responseStream.WriteAsync(new RaceMessage { Count = ++sent });
                }

                await readTask;

                _logger.LogInformation($"Iteration {i} - Finish");
            }

            _logger.LogInformation($"ReadySetGo finished");
        }
    }
}
