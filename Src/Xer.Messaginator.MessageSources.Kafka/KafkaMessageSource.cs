using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Xer.Messaginator.MessageSources.Kafka.Entities
{
    public class KafkaMessageSource<TMessage> : MessageSource<TMessage> where TMessage : class
    {
        private ConcurrentQueue<TMessage> _queuedMessages = new ConcurrentQueue<TMessage>();
        private readonly Consumer<Null, TMessage> _consumer;
        private Timer _timer;

        public TimeSpan PollDuration { get; }
        protected bool ShouldStopPolling { get; private set; }

        public KafkaMessageSource(Consumer<Null, TMessage> consumer, TimeSpan pollDuration, TimeSpan queuedMessagePublishInterval)
        {
            _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
            // Subscribe to kafka consumer events.
            _consumer.OnMessage += OnKafkaMessageReceived;
            _consumer.OnError += (_, kafkaError) => PublishKafkaError(kafkaError);
            _consumer.OnConsumeError += (_, kafkaMessage) => PublishKafkaError(kafkaMessage.Error);
            
            PollDuration = pollDuration;

            // Publish queued message every 500 milliseconds.
            _timer = new Timer(PublishQueuedMessages, _queuedMessages, 0, queuedMessagePublishInterval.Milliseconds);
        }

        public override Task ReceiveAsync(MessageContainer<TMessage> message, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (message == null)
            {
                return Task.FromException(new ArgumentNullException(nameof(message)));
            }

            _queuedMessages.Enqueue(message);
            
            return Task.CompletedTask;
        }

        public override Task StartReceivingAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            ShouldStopPolling = false;
            
            // Not awaited. This will execute in background.
            StartPollingAsync(cancellationToken);

            return Task.CompletedTask;
        }

        protected virtual Task StartPollingAsync(CancellationToken cancellationToken)
        {
            try 
            {
                while (!ShouldStopPolling)
                {
                    _consumer.Poll(PollDuration);                
                }
            }
            catch (Exception ex)
            {
                PublishException(ex);
            }

            return Task.CompletedTask;
        }

        public override Task StopReceivingAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            ShouldStopPolling = true;
            return Task.CompletedTask;
        }

        protected virtual void OnKafkaMessageReceived(object sender, Message<Null, TMessage> kafkaMessage)
        {
            try
            {
                PublishMessage(new MessageContainer<TMessage>(kafkaMessage.Value));
            }
            catch (Exception ex)
            {
                PublishException(ex);
            }
        }

        protected virtual void PublishKafkaError(Error kafkaError)
        {
            PublishException(new KafkaMessageSourceException(kafkaError));
        }

        private void PublishQueuedMessages(object state)
        {
            ConcurrentQueue<TMessage> queuedMessages = state as ConcurrentQueue<TMessage>;
            if (queuedMessages == null)
            {
                return;
            }

            do 
            {
                if (queuedMessages.TryDequeue(out TMessage queuedMessage))
                {
                    PublishMessage(new MessageContainer<TMessage>(queuedMessage));
                }
            }
            while (!queuedMessages.IsEmpty);
        }
    }
}