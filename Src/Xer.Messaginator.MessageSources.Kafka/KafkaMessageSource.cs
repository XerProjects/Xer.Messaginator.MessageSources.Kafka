using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Xer.Messaginator.MessageSources.Kafka.Exceptions;

namespace Xer.Messaginator.MessageSources.Kafka
{
    
    /// <summary>
    /// Kafka message source that has a key and a value.
    /// </summary>
    public class KafkaMessageSource<TKey, TValue> : MessageSource<TValue>, IDisposable where TValue : class
    {
        private readonly ConcurrentQueue<MessageContainer<TValue>> _queuedMessages = new ConcurrentQueue<MessageContainer<TValue>>();
        private readonly Consumer<TKey, TValue> _consumer;
        private readonly Timer _timer;

        public TimeSpan PollDuration { get; }
        public TimeSpan PublishQueuedMessagesInterval { get; }
        protected bool ShouldStopPolling { get; private set; }
        
        public KafkaMessageSource(KafkaConsumerProperties<TKey, TValue> properties, TimeSpan pollDuration, TimeSpan publishQueuedMessagesInterval)
        {
            _consumer = new Consumer<TKey, TValue>(properties, properties.KeyDeserializer, properties.ValueDeserializer);
            _consumer.Subscribe(properties.Topics);

            // Subscribe to kafka consumer events.
            _consumer.OnMessage += OnKafkaMessageReceived;
            _consumer.OnError += (_, kafkaError) => PublishKafkaError(kafkaError);
            _consumer.OnConsumeError += (_, kafkaMessage) => PublishKafkaError(kafkaMessage.Error);
            
            PollDuration = pollDuration;
            PublishQueuedMessagesInterval = publishQueuedMessagesInterval;

            // Publish queued message every configured.
            _timer = new Timer(PublishQueuedMessages, _queuedMessages, 0, publishQueuedMessagesInterval.Milliseconds);
        }

        public override Task ReceiveAsync(MessageContainer<TValue> message, CancellationToken cancellationToken = default(CancellationToken))
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

        protected virtual void OnKafkaMessageReceived(object sender, Message<TKey, TValue> kafkaMessage)
        {
            try
            {
                PublishMessage(new MessageContainer<TValue>(kafkaMessage.Value));
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
            ConcurrentQueue<MessageContainer<TValue>> queuedMessages = state as ConcurrentQueue<MessageContainer<TValue>>;
            if (queuedMessages == null)
            {
                return;
            }

            int messageCount = 0;

            do 
            {
                if (queuedMessages.TryDequeue(out MessageContainer<TValue> queuedMessage))
                {
                    PublishMessage(queuedMessage);
                    messageCount++;
                }
            }
            // Publish 10 queued messages per tick.
            while (messageCount <= 10 && !queuedMessages.IsEmpty);
        }

        public void Dispose()
        {
            _consumer.Dispose();
            _timer.Dispose();
        }
    }
}