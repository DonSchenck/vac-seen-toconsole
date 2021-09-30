using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;
//using Newtonsoft.Json;
using System.Collections.Generic;
using KubeServiceBinding;

namespace vac_seen_toconsole
{
    class Program
    {
        private const string KafkaTopic = "us";

        static void Main(string[] args)
        {
            // Consume Kafka events and write them to Console
            Console.WriteLine("vac-seen-toconsole started.");

            Dictionary<string,string> bindingsKVP = GetDotnetServiceBindings();
            bool cancelled = false;
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.Token;
            var config = new ConsumerConfig
            {
                BootstrapServers = bindingsKVP["bootstrapservers"],
                GroupId = "foo",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe("us");
                
                while (!cancelled)
                {
                    var consumeResult = consumer.Consume(token);
                    Console.WriteLine(consumeResult);
                }

                consumer.Close();
            }

        }
        private static Dictionary<string,string> GetDotnetServiceBindings() {
            int count = 0;
            int maxTries = 999;
            while(true) {
                try {
                    DotnetServiceBinding sc = new DotnetServiceBinding();
                    Dictionary<string,string> d = sc.GetBindings("kafka");
                    return d;
                    // At this point, we have the information needed to bind to our Kafka
                    // bootstrap server.
                } catch (Exception e) {
                    // handle exception
                    System.Threading.Thread.Sleep(1000);
                    if (++count == maxTries) throw e;
                }
            }
        }
    }
}
