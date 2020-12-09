using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using Azure.Messaging.ServiceBus;

namespace OverrideAglomeraFunc
{
    public static class AnaliseAglomeracoes
    {
        private static string SERVICEBUS_CONNECTION_STRING = "<>";
        private static string SERVICEBUS_queueName = "<>";
        
        private static string COMPUTER_VISION_subscriptionKey = Environment.GetEnvironmentVariable("COMPUTER_VISION_SUBSCRIPTION_KEY");
        private static string COMPUTER_VISION_endpoint = Environment.GetEnvironmentVariable("COMPUTER_VISION_ENDPOINT");

        private static string uriBase = COMPUTER_VISION_endpoint + "vision/v3.1/analyze";

        [FunctionName("AnaliseAglomeracoes")]
        public static Task Run([EventGridTrigger] EventGridEvent eventGridEvent,
            [Blob("{data.url}", FileAccess.Read)] Stream input,
            ILogger log)
        {
            try
            {
                if (input != null)
                {
                    var createdEvent = ((JObject)eventGridEvent.Data).ToObject<StorageBlobCreatedEventData>();

                    MakeAnalysisRequest(input, createdEvent.Url, log).Wait();
                }
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.Message);
                throw;
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="imagem"></param>
        /// <returns></returns>
        static async Task MakeAnalysisRequest(Stream imagem, string eventUrl, ILogger log)
        {
            try
            {
                HttpClient client = new HttpClient();

                // Request headers.
                client.DefaultRequestHeaders.Add(
                    "Ocp-Apim-Subscription-Key", COMPUTER_VISION_subscriptionKey);

                // O método Analyze Image retorna informações sobre o seguinte:
                // Categories:  categoriza o conteúdo da imagem de acordo com uma
                //              taxonomia definida na documentação.
                // Description: descreve o conteúdo da imagem com um completo
                //              frase em idiomas suportados.
                // Color:       determina a cor de destaque, cor dominante,
                //              e se uma imagem é preto e branco.
                string requestParameters =
                    "visualFeatures=Categories,Description,Color";

                string uri = uriBase + "?" + requestParameters;

                HttpResponseMessage response;

                BinaryReader binaryReader = new BinaryReader(imagem);
                byte[] byteData = binaryReader.ReadBytes((int)imagem.Length);

                using (ByteArrayContent content = new ByteArrayContent(byteData))
                {
                    content.Headers.ContentType =
                        new MediaTypeHeaderValue("application/octet-stream");

                    // chamada da API.
                    response = await client.PostAsync(uri, content);
                }

                // resposta em JSON.
                string contentString = await response.Content.ReadAsStringAsync();

                log.LogInformation("\nResponse:\n\n{0}\n",
                    JToken.Parse(contentString).ToString());

                try
                {
                    var retorno = JToken.Parse(contentString);
                    var temMultidao = retorno["categories"].ToString().Contains("crowd");
                    if (temMultidao)
                    {
                        log.LogInformation("Aglomeracao identificada, enviando mensagem no servicebus");
                        SendMessageAsync(eventUrl, log).Wait();
                    }
                }
                catch (Exception e)
                {
                    log.LogInformation(e.Message);
                }
            }
            catch (Exception e)
            {
                log.LogInformation(e.Message);
            }
        }

        static async Task SendMessageAsync(string imagemName, ILogger log)
        {
            // create a Service Bus client 
            await using (ServiceBusClient client = new ServiceBusClient(SERVICEBUS_CONNECTION_STRING))
            {
                // create a sender for the queue 
                ServiceBusSender sender = client.CreateSender(SERVICEBUS_queueName);

                // create a message that we can send
                ServiceBusMessage message = new ServiceBusMessage($"Aglomeração identificada na imagem {imagemName}");

                // send the message
                await sender.SendMessageAsync(message);
                log.LogInformation($"Sent a single message to the queue: {SERVICEBUS_queueName}");
            }
        }
    }
}
