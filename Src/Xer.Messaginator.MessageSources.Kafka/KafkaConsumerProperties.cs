using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Xer.Messaginator.MessageSources.Kafka
{
    /// <summary>
    /// Properties for a Kafka consumer that has Null key.
    /// </summary>
    public class KafkaConsumerProperties<TValue> : KafkaConsumerProperties<Null, TValue> where TValue : class
    {
        public KafkaConsumerProperties(string topic, IDeserializer<TValue> valueDeserializer) 
            : this(new[] { topic }, valueDeserializer)
        {
        }

        public KafkaConsumerProperties(IEnumerable<string> topics, IDeserializer<TValue> valueDeserializer) 
            : base(topics, new NullDeserializer(), valueDeserializer)
        {
        }
    }

    /// <summary>
    /// Properties for a Kafka consumer that has a key.
    /// </summary>
    public class KafkaConsumerProperties<TKey, TValue> : IEnumerable<KeyValuePair<string, object>> where TValue : class
    {
        private Dictionary<string, object> _properties = new Dictionary<string, object>();
        public const string TopicsKey = "topics";
        public const string KeyDeserializerKey = "key.deserializer";
        public const string ValueDeserializerKey = "value.deserializer";

        public IReadOnlyCollection<string> Topics
        {
            get => GetPropertyOrDefault<IReadOnlyCollection<string>>(TopicsKey, new List<string>());
            private set => TryAddProperty(TopicsKey, value);
        }

        public IDeserializer<TKey> KeyDeserializer 
        { 
            get => GetPropertyOrDefault<IDeserializer<TKey>>(KeyDeserializerKey, default(IDeserializer<TKey>));
            private set => TryAddProperty(KeyDeserializerKey, value);
        }
        
        public IDeserializer<TValue> ValueDeserializer
        { 
            get => GetPropertyOrDefault<IDeserializer<TValue>>(ValueDeserializerKey, default(IDeserializer<TValue>));
            private set => TryAddProperty(ValueDeserializerKey, value);
        }

        public KafkaConsumerProperties(string topic, IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer) 
            : this(new[] { topic }, keyDeserializer, valueDeserializer)
        {
        }

        public KafkaConsumerProperties(IEnumerable<string> topics, IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
        {
            Topics = topics.ToList();
            KeyDeserializer = keyDeserializer;
            ValueDeserializer = valueDeserializer;
        }

        public T GetPropertyOrDefault<T>(string key, T defaultValue = default(T))
        {
            if (_properties.TryGetValue(key, out var storedValule) && 
                storedValule is T typedValue)
            {
                return typedValue;
            }

            return defaultValue;
        }

        public bool TryGetProperty<T>(string key, out T propertyValue)
        {
            if (_properties.TryGetValue(key, out var storedValue) && 
                storedValue is T typedValue)
            {
                propertyValue = typedValue;
                return true;
            }

            propertyValue = default(T);
            return false;
        }

        public bool TryAddProperty<T>(string key, T value)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentException("Key connot be null or empty", nameof(key));
            }

            if (!TryGetProperty<T>(key, out var storedValue))
            {
                return false;
            }

            _properties.Add(key, value);
            return true;
        }

        IEnumerator<KeyValuePair<string, object>> IEnumerable<KeyValuePair<string, object>>.GetEnumerator()
        {
            return _properties.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _properties.GetEnumerator();
        }
    }
}