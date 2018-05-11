using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Xer.Messaginator.MessageSources.Kafka.Entities
{
    public class KafkaMessageSource : MessageSource<Message>
    {
        private readonly Consumer _consumer;
        private readonly string _topic;
        private readonly int _milSecondsInterval;
        private bool stop;
        private CancellationToken requestedCancellationToken;
        private Task consumerPollingTask;

        public KafkaMessageSource( Consumer consumer,
                                   string topic, 
                                   int milSecondsInterval )
        {
            _consumer = consumer;
            _topic = topic;
            _milSecondsInterval = milSecondsInterval;

            _consumer.Subscribe(_topic);
            
        }

        public override Task ReceiveAsync(MessageContainer<Message> message, CancellationToken cancellationToken = default(CancellationToken))
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
            stop = false;
            
            consumerPollingTask = ProcessNextMessageAsync(cancellationToken);

            return Task.CompletedTask;
        }

        public override Task StopReceivingAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            stop = true;

            return consumerPollingTask;
        }

        private async Task ProcessNextMessageAsync(CancellationToken cancellationToken)
        {
            try 
            {
                _consumer.OnMessage += (_, msg)
                                    => ReceiveAsync(new MessageContainer<Message>(msg));

                while (!stop)
                {
                    _consumer.Poll(_milSecondsInterval);                
                }
            }
            catch (Exception ex)
            {
                PublishException(ex);
            }
        }

    }
}