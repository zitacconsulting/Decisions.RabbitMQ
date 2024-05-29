using System.Collections.Generic;
using System.Runtime.Serialization;
using RabbitMQ.Client.Events;
using DecisionsFramework.ServiceLayer.Services.ContextData;
using System.Text;

namespace Zitac.RabbitMQ.Steps
{
    [DataContract]
    public class RabbitMQResult
    {
        [DataMember]
        public string? Body { get; set; }

        [DataMember]
        public string? ContentType { get; set; }

        [DataMember]
        public DataPair[]? Headers { get; set; }

        public RabbitMQResult() { }

        public RabbitMQResult(BasicDeliverEventArgs eventArgs)
        {
            if (!eventArgs.Body.IsEmpty) 
            { 
                this.Body = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
            }
            this.ContentType = eventArgs.BasicProperties.ContentType;

            var dataPairs = new List<DataPair>();
            if (eventArgs.BasicProperties.Headers != null)
            {
                foreach (var header in eventArgs.BasicProperties.Headers)
                {
                    var value = Encoding.UTF8.GetString((byte[])header.Value);
                    dataPairs.Add(new DataPair(header.Key, value));
                }
            }
            this.Headers = dataPairs.ToArray();
        }
    }
}
