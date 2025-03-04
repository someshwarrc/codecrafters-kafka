using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using App.Kafka;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage

class KafkaServer {
    private static Channel<Func<Task>> _channel = Channel.CreateUnbounded<Func<Task>>();
    private static TcpClient client;
    public static async Task EnqueueRequest(Func<Task> request)
    {
        await _channel.Writer.WriteAsync(request);
    }

    public static async Task StartProcessingAsync()
    {
        while (await _channel.Reader.WaitToReadAsync())
        {
            while (_channel.Reader.TryRead(out var request))
            {
                NetworkStream stream = client.GetStream();
                await stream.ParseApiVersionRequest();
            }
        }
    }    
    public static async Task main() {
        TcpListener server = new TcpListener(IPAddress.Any, 9092);
        server.Start();
        try {
            while(true){
                TcpClient client = await server.AcceptTcpClientAsync();
                KafkaServer.client = client;
                _=KafkaServer.StartProcessingAsync();
            }
        } catch (Exception e) {
            Console.WriteLine(e);
            server.Stop();
        }

    }
}

