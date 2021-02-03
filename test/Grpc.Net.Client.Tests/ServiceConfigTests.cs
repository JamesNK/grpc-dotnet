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
using System.Collections.Generic;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.ServiceConfig;
using NUnit.Framework;
using GrpcMethodConfig = Grpc.ServiceConfig.MethodConfig;
using GrpcServiceConfig = Grpc.ServiceConfig.ServiceConfig;

namespace Grpc.Net.Client.Tests
{
    [TestFixture]
    public class ServiceConfigTests
    {
        [Test]
        public void sdfsdfsdf()
        {
            var sc = new GrpcServiceConfig();
            sc.RetryThrottling = new GrpcServiceConfig.Types.RetryThrottlingPolicy
            {
                
            };
            sc.MethodConfig.Add(new GrpcMethodConfig
            {
                Name =
                {
                    new GrpcMethodConfig.Types.Name { Method = "method2", Service = "service2" },
                    new GrpcMethodConfig.Types.Name { Method = "method1", Service = "service1" }
                },
                RetryPolicy = new GrpcMethodConfig.Types.RetryPolicy
                {
                    InitialBackoff = Duration.FromTimeSpan(TimeSpan.FromSeconds(1)),
                    MaxAttempts = 1,
                    RetryableStatusCodes =
                    {
                        Google.Rpc.Code.Aborted
                    }
                }
            });
            sc.LoadBalancingConfig.Add(new LoadBalancingConfig
            {
                RoundRobin = new RoundRobinConfig
                {
                }
            });
            sc.MethodConfig.Add(new GrpcMethodConfig());
            sc.HealthCheckConfig = new GrpcServiceConfig.Types.HealthCheckConfig
            {
                ServiceName = "blah"
            };

            var json = Google.Protobuf.JsonFormatter.Default.Format(sc);

            Console.WriteLine(json);
            //Grpc.ServiceConfig.ServiceConfig.Descriptor.

            //Google.Protobuf.
            //            Greet.
            //global::Grpc.ServiceConfig.
        }

        /*
{
  "methodConfig": [
    {
      "name": [
        {
          "service": "service2",
          "method": "method2"
        },
        {
          "service": "service1",
          "method": "method1"
        }
      ],
      "retryPolicy": {
        "maxAttempts": 1,
        "initialBackoff": "1s",
        "retryableStatusCodes": [
          "ABORTED"
        ]
      }
    },
    {}
  ],
  "retryThrottling": {},
  "loadBalancingConfig": [
    {
      "round_robin": {}
    }
  ],
  "healthCheckConfig": {
    "serviceName": "blah"
  }
}
        */

        [Test]
        public void ServiceConfig_CreateUnderlyingConfig()
        {
            // Arrange & Act
            var serviceConfig = new ServiceConfig
            {
                MethodConfigs =
                {
                    new MethodConfig
                    {
                        Names = { new Name() },
                        RetryPolicy = new RetryThrottlingPolicy
                        {
                            MaxAttempts = 5,
                            InitialBackoff = TimeSpan.FromSeconds(1),
                            RetryableStatusCodes = { StatusCode.Unavailable, StatusCode.Aborted }
                        }
                    }
                }
            };

            // Assert
            Assert.AreEqual(1, serviceConfig.MethodConfigs.Count);
            Assert.AreEqual(1, serviceConfig.MethodConfigs[0].Names.Count);
            Assert.AreEqual(5, serviceConfig.MethodConfigs[0].RetryPolicy!.MaxAttempts);
            Assert.AreEqual(TimeSpan.FromSeconds(1), serviceConfig.MethodConfigs[0].RetryPolicy!.InitialBackoff);
            Assert.AreEqual(StatusCode.Unavailable, serviceConfig.MethodConfigs[0].RetryPolicy!.RetryableStatusCodes[0]);
            Assert.AreEqual(StatusCode.Aborted, serviceConfig.MethodConfigs[0].RetryPolicy!.RetryableStatusCodes[1]);

            var inner = serviceConfig.Inner;
            var methodConfigs = (IList<object>)inner["method_config"];
            var allServices = (IDictionary<string, object>)methodConfigs[0];

            Assert.AreEqual(5, (int)((IDictionary<string, object>)allServices["retryPolicy"])["maxAttempts"]);
            Assert.AreEqual("1s", (string)((IDictionary<string, object>)allServices["retryPolicy"])["initialBackoff"]);
            Assert.AreEqual("UNAVAILABLE", (string)((IList<object>)((IDictionary<string, object>)allServices["retryPolicy"])["retryableStatusCodes"])[0]);
            Assert.AreEqual("ABORTED", (string)((IList<object>)((IDictionary<string, object>)allServices["retryPolicy"])["retryableStatusCodes"])[1]);
        }

        [Test]
        public void ServiceConfig_ReadUnderlyingConfig()
        {
            // Arrange
            var inner = new Dictionary<string, object>
            {
                ["method_config"] = new List<object>
                {
                    new Dictionary<string, object>
                    {
                        ["name"] = new List<object> { new Dictionary<string, object>() },
                        ["retryPolicy"] = new Dictionary<string, object>
                        {
                            ["maxAttempts"] = 5,
                            ["initialBackoff"] = "1s",
                            ["retryableStatusCodes"] = new List<object> { "UNAVAILABLE", "ABORTED" }
                        }
                    }
                }
            };

            // Act
            var serviceConfig = new ServiceConfig(inner);

            // Assert
            Assert.AreEqual(1, serviceConfig.MethodConfigs.Count);
            Assert.AreEqual(1, serviceConfig.MethodConfigs[0].Names.Count);
            Assert.AreEqual(5, serviceConfig.MethodConfigs[0].RetryPolicy!.MaxAttempts);
            Assert.AreEqual(TimeSpan.FromSeconds(1), serviceConfig.MethodConfigs[0].RetryPolicy!.InitialBackoff);
            Assert.AreEqual(StatusCode.Unavailable, serviceConfig.MethodConfigs[0].RetryPolicy!.RetryableStatusCodes[0]);
            Assert.AreEqual(StatusCode.Aborted, serviceConfig.MethodConfigs[0].RetryPolicy!.RetryableStatusCodes[1]);
        }

        [Test]
        public void RetryThrottlingPolicy_ReadUnderlyingConfig_Success()
        {
            // Arrange
            var inner = new Dictionary<string, object>
            {
                ["initialBackoff"] = "1.1s",
                ["retryableStatusCodes"] = new List<object> { "UNAVAILABLE", "Aborted", 1 }
            };

            // Act
            var retryPolicy = new RetryThrottlingPolicy(inner);

            // Assert
            Assert.AreEqual(TimeSpan.FromSeconds(1.1), retryPolicy.InitialBackoff);
            Assert.AreEqual(StatusCode.Unavailable, retryPolicy.RetryableStatusCodes[0]);
            Assert.AreEqual(StatusCode.Aborted, retryPolicy.RetryableStatusCodes[1]);
            Assert.AreEqual(StatusCode.Cancelled, retryPolicy.RetryableStatusCodes[2]);
        }

        [Test]
        public void Name_AllServicesIsReadOnly_ErrorOnChange()
        {
            // Arrange & Act & Assert
            Assert.Throws<NotSupportedException>(() => Name.AllServices.Method = "This will break");
        }
    }
}
