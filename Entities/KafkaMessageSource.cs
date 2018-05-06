using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Protocol;
using Xer.Messaginator.MessageSources.Queue;

namespace Xer.Messaginator.MessageSources.Kafka.Entities
{
    public class KafkaMessageSource : MessageSource<IEnumerable<Message>>
    {
        private Consumer _kafkaConsumer = null;
        private Task _consumerTask = null;

        public KafkaMessageSource ( Consumer consumer )
        {
            _kafkaConsumer = consumer;
        }

        public override Task ReceiveAsync(MessageContainer<IEnumerable<Message>> message, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (message == null)
            {
                return Task.FromException(new ArgumentNullException(nameof(message)));
            }

            PublishMessage(message);
            
            return Task.CompletedTask;
        }

        public override Task StartReceivingAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            _consumerTask = DequeueAsync(cancellationToken);

            return Task.CompletedTask;
        }

        public override Task StopReceivingAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            return _consumerTask;
        }

        private Task<MessageContainer<IEnumerable<Message>>> DequeueAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            IEnumerable<Message> message = _kafkaConsumer.Consume();
            
            return Task.FromResult(new MessageContainer<IEnumerable<Message>>(message));
        }
    }
}