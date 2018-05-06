using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Protocol;

namespace Xer.Messaginator.MessageSources.Kafka.Entities
{
    public abstract class KafkaMessageSender : MessageSource<IEnumerable<Message>>
    {
        private Producer _producer;
        
        public KafkaMessageSender(Producer producer)
        {

            _producer = producer;

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
            //TODO: not sure about this. Since this is not needed in Sender Processor
            return Task.CompletedTask;
        }

        public override Task StopReceivingAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            _producer.Stop();
            return Task.CompletedTask;            
        }

        public abstract Task SendMessageAsync(MessageContainer<IEnumerable<Message>> message, CancellationToken cancellationToken = default(CancellationToken));
        
    }
}