using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using System.Threading.Tasks;
using App.Kafka;

class KafkaServer
{
    private static readonly Channel<TcpClient> _channel = Channel.CreateUnbounded<TcpClient>();
    
    public static async Task EnqueueClient(TcpClient client)
    {
        await _channel.Writer.WriteAsync(client);
    }

    public static async Task StartProcessingAsync()
    {
        while (await _channel.Reader.WaitToReadAsync())
        {
            while (_channel.Reader.TryRead(out var client))
            {
                try
                {
                    Console.WriteLine("Processing client request...");
                    using NetworkStream stream = client.GetStream();
                    await stream.ParseApiVersionRequest();
                    client.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing request: {ex.Message}");
                }
            }
        }
    }

    public static async Task Main()
    {
        TcpListener server = new TcpListener(IPAddress.Any, 9092);
        server.Start();
        Console.WriteLine("Kafka Server started on port 9092...");

        // Start the processing task in the background
        _ = Task.Run(StartProcessingAsync);

        try
        {
            while (true)
            {
                TcpClient client = await server.AcceptTcpClientAsync();
                Console.WriteLine("New client connected...");
                await EnqueueClient(client);
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"Server error: {e.Message}");
            server.Stop();
        }
    }
}
