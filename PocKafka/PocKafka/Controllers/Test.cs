using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Text.Json;
using System.Threading.Tasks;

namespace PocKafka.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class Test : ControllerBase
    {
        private static int count;

        public async Task<IActionResult> Get()
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var p = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    ++count;
                    var msg2 = new Message2 { Description = $"Description {count}", Name = $"Name {count}" };
                    var msg = new Message<Null, string> { Value = JsonSerializer.Serialize(msg2) };
                    var dr = await p.ProduceAsync("test-topic", msg);
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
            return Ok();
        }
    }
}
