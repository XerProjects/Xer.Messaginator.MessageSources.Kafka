using System;
using Confluent.Kafka;

namespace Xer.Messaginator.MessageSources.Kafka
{
    public class KafkaMessageSourceException : Exception
    {
        public KafkaMessageSourceException(Error kafkaError) : base(EnsureNotNull(kafkaError))
        {
        }

        public KafkaMessageSourceException(string message) : base(message)
        {
        }

        public KafkaMessageSourceException(string message, Exception innerException) : base(message, innerException)
        {
        }

        private static string EnsureNotNull(Error kafkaError)
        {
            // Create kafka error message.
            return $"Kafka Error: {kafkaError.Code}|{kafkaError.Reason}";
        }
    }
}