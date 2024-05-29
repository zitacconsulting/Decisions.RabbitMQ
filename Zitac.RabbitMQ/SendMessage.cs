using DecisionsFramework.Design.Flow;
using DecisionsFramework.Design.Flow.CoreSteps;
using DecisionsFramework.Design.Flow.Mapping;
using DecisionsFramework.Design.Flow.Mapping.InputImpl;
using DecisionsFramework.Design.Properties;
using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.ServiceLayer.Services.ContextData;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using DecisionsFramework.Data.DataTypes;
using System.Text;

namespace Zitac.RabbitMQ.Steps;

[AutoRegisterStep("Send Message", "Integration", "RabbitMQ", "Zitac")]
[Writable]
public class SendMessage : BaseFlowAwareStep, ISyncStep, IDataConsumer, IDataProducer, IDefaultInputMappingStep
{
    [WritableValue]
    private bool expectResponse;

    [PropertyClassification(0, "Expect Response", new string[] { "Settings" })]
    public bool ExpectResponse
    {
        get { return expectResponse; }
        set
        {
            expectResponse = value;
            this.OnPropertyChanged("InputData");
            this.OnPropertyChanged("OutcomeScenarios");
        }
    }

    public IInputMapping[] DefaultInputs
    {
        get
        {
            return new IInputMapping[]
            {
                new ConstantInputMapping { InputDataName = "Port", Value = "5672" },
                new ConstantInputMapping { InputDataName = "Content Type", Value = "application/json" },
                new ConstantInputMapping { InputDataName = "Use SSL", Value = true },
                new ConstantInputMapping { InputDataName = "Timeout in Sec", Value = 10 }
            };
        }
    }

    public DataDescription[] InputData
    {
        get
        {
            List<DataDescription> dataDescriptionList = new List<DataDescription>();

            // Connection Settings
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(String)), "Server") { Categories = new string[] { "Connection Settings" } });
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(Int32)), "Port") { Categories = new string[] { "Connection Settings" } });
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(Credentials)), "Credentials") { Categories = new string[] { "Connection Settings" } });
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(bool)), "Use SSL") { Categories = new string[] { "Connection Settings" } });

            // Outgoing Message
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(String)), "Outgoing Queue Name") { Categories = new string[] { "Outgoing Message" } });
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(String)), "Payload") { Categories = new string[] { "Outgoing Message" } });
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(SimpleKeyValuePair)), "Application Properties", true, true, false) { Categories = new string[] { "Outgoing Message" } });
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(String)), "Content Type") { Categories = new string[] { "Outgoing Message" } });

            // Response
            if (expectResponse)
            {
                dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(String)), "Response Queue Name") { Categories = new string[] { "Response" } });
                dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(String)), "Correlation ID") { Categories = new string[] { "Response" } });
                dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(Int32)), "Timeout in Sec") { Categories = new string[] { "Response" } });
            }
            return dataDescriptionList.ToArray();
        }
    }

    public override OutcomeScenarioData[] OutcomeScenarios
    {
        get
        {
            List<OutcomeScenarioData> outcomeScenarioDataList = new List<OutcomeScenarioData>();

            if (expectResponse)
            {
                outcomeScenarioDataList.Add(new OutcomeScenarioData("Done", new DataDescription(typeof(RabbitMQResult), "Result")));
                outcomeScenarioDataList.Add(new OutcomeScenarioData("Timeout"));
            }
            else
            {
                outcomeScenarioDataList.Add(new OutcomeScenarioData("Done"));
            }
            outcomeScenarioDataList.Add(new OutcomeScenarioData("Error", new DataDescription(typeof(string), "Error Message")));
            return outcomeScenarioDataList.ToArray();
        }
    }

    public ResultData Run(StepStartData data)
    {
        // Connection Settings
        string server = (string)data.Data["Server"];
        int port = (int)data.Data["Port"];
        Credentials credentials = (Credentials)data.Data["Credentials"];
        bool useSSL = (bool)data.Data["Use SSL"];

        // Outgoing Message
        string queueName = (string)data.Data["Outgoing Queue Name"];
        string payload = (string)data.Data["Payload"];
        SimpleKeyValuePair[] applicationProperties = (SimpleKeyValuePair[])data.Data["Application Properties"];
        string contentType = (string)data.Data["Content Type"];

        // Response
        string correlationID = (string)data.Data["Correlation ID"];
        string responseQueueName = (string)data.Data["Response Queue Name"];
        int? timeOutInSec = (int?)data.Data["Timeout in Sec"];

        var factory = new ConnectionFactory()
        {
            HostName = server,
            Port = port,
            UserName = credentials.Username,
            Password = credentials.Password,
            Ssl = { Enabled = useSSL }
        };

        try
        {
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var properties = channel.CreateBasicProperties();
                if (applicationProperties != null)
                {
                    foreach (var keyValuePair in applicationProperties)
                    {
                        properties.Headers ??= new Dictionary<string, object>();
                        properties.Headers[keyValuePair.Key] = keyValuePair.Value;
                    }
                }

                properties.ContentType = contentType;

                if (expectResponse)
                {
                    properties.CorrelationId = correlationID;
                }

                var messageBody = Encoding.UTF8.GetBytes(payload);
                channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: properties, body: messageBody);

                if (expectResponse)
                {
                    var consumer = new EventingBasicConsumer(channel);
                    var responseReceived = false;
                    var response = default(BasicDeliverEventArgs);

                    consumer.Received += (model, ea) =>
                    {
                        if (ea.BasicProperties.CorrelationId == correlationID)
                        {
                            response = ea;
                            responseReceived = true;
                        }
                    };

                    channel.BasicConsume(queue: responseQueueName, autoAck: true, consumer: consumer);

                    var timeout = timeOutInSec ?? 10;
                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                    while (!responseReceived && stopwatch.Elapsed.TotalSeconds < timeout)
                    {
                        System.Threading.Thread.Sleep(100);
                    }

                    if (responseReceived)
                    {
                        var result = new RabbitMQResult(response);
                        var dictionary = new Dictionary<string, object> { { "Result", result } };
                        return new ResultData("Done", dictionary);
                    }
                    else
                    {
                        return new ResultData("Timeout");
                    }
                }

                return new ResultData("Done");
            }
        }
        catch (Exception e)
        {
            var exceptionMessage = e.ToString();
            var errorDict = new Dictionary<string, object> { { "Error Message", exceptionMessage } };
            return new ResultData("Error", errorDict);
        }
    }
}
