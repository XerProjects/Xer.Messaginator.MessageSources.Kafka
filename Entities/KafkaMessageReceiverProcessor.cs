using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Protocol;
using KafkaNet;
using KafkaNet.Model;

namespace Xer.Messaginator.MessageSources.Kafka.Entities
{
    public class KafkaMessageReceiverProcessor : MessageProcessor<IEnumerable<Message>> , ISupportMessageForwarding
    {
       private IMessageForwarder _messageForwarder;
       private string _nextProcessor;
        public override string Name => GetType().Name;

        public KafkaMessageReceiverProcessor(IMessageSource<IEnumerable<Message>> messageSource, string nextProcessor) 
        : base(messageSource)
        {
            _nextProcessor = nextProcessor;
        }

        protected override Task ProcessMessageAsync(MessageContainer<IEnumerable<Message>> receivedMessage, CancellationToken cancellationToken)
        {            
            return _messageForwarder.ForwardMessageAsync(_nextProcessor, receivedMessage, cancellationToken);
        }

        public void SetMessageForwarder(IMessageForwarder messageForwarder)
        {
            _messageForwarder = messageForwarder;
        }
    }
}