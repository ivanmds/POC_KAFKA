using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace PocKafka.Consumers
{
    public class Worker : BackgroundService
    {
        private IConsumer<Ignore, string> consumer;
        private readonly IServiceProvider services;

        public Worker(IServiceProvider services)
        {
            this.services = services;
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group2",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            consumer = new ConsumerBuilder<Ignore, string>(conf).Build();
            consumer.Subscribe("test-topic");
            await base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var scope = services.CreateScope();
            await Task.Run(() =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var cr = consumer.Consume(stoppingToken);
                    Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                }
            });
        }

        public override void Dispose()
        {
            consumer?.Close();
            base.Dispose();
        }
    }
}
