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
        private Task consumerTask;
        private CancellationToken requestedCancellationToken;
        private ConsumerState consumerState;
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
            requestedCancellationToken = cancellationToken;

            if (consumerState != ConsumerState.Running)
            {
                consumerState = ConsumerState.Running;

                while ( consumerState == ConsumerState.Running && 
                        !requestedCancellationToken.IsCancellationRequested)
                {
                    consumerTask = ProcessNextMessageAsync(requestedCancellationToken);
                }
            }

            return Task.CompletedTask;
        }

        public override Task StopReceivingAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (consumerState != ConsumerState.Stopped)
            {
                consumerState = ConsumerState.Stopped;                                                
            }

            return consumerTask;
        }

        private async Task ProcessNextMessageAsync(CancellationToken cancellationToken)
        {
            try 
            {
                MessageContainer<Message> message = await GetNextMessageAsync(cancellationToken).ConfigureAwait(false);

                PublishMessage(message);
            }
            catch (Exception e)
            {
                _consumer.OnError += (_, err) => PublishException(e);

                _consumer.OnConsumeError += (_, err) => PublishException(e);
                
            }
        }

        private Task<MessageContainer<Message>> GetNextMessageAsync (CancellationToken cancellationToken)
        {
            if (_consumer.Consume( out Message msg, _milSecondsInterval ))
            {
                return Task.FromResult(new MessageContainer<Message>(msg));
            }

            return Task.FromResult(MessageContainer<Message>.Empty);
        }

        private enum ConsumerState 
        {
            Stopped,
            Running
        }
    }
}