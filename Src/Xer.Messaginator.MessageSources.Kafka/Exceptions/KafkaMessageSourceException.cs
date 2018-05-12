using System;
using Confluent.Kafka;

namespace Xer.Messaginator.MessageSources.Kafka.Exceptions
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
            if (kafkaError == null)
            {
                throw new ArgumentNullException(nameof(kafkaError));
            }
            
            // Create kafka error message.
            return $"Kafka Error: {kafkaError.Code} | {kafkaError.ToString()}";
        }
    }
}