using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Protocol;

namespace Xer.Messaginator.MessageSources.Kafka.Entities
{
    public class KafkaMessageSenderProcessor : MessageProcessor<IEnumerable<Message>>
    {
        private KafkaMessageSender _kafkaSender;
        public KafkaMessageSenderProcessor(IMessageSource<IEnumerable<Message>> messageSource) 
        : base(messageSource)
        {
            _kafkaSender = messageSource as KafkaMessageSender;
        }

        public override string Name => GetType().Name;

        protected override Task ProcessMessageAsync(MessageContainer<IEnumerable<Message>> receivedMessage, CancellationToken cancellationToken)
        {
            return _kafkaSender.SendMessageAsync(receivedMessage);
        }
    }
}