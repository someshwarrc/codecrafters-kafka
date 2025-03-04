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
                    
                    if (!stream.CanRead)
                    {
                        Console.WriteLine("Client closed connection before request was received.");
                        continue;
                    }

                    // Attempt to read and process the request safely
                    try
                    {
                        await stream.ParseApiVersionRequest();
                    }
                    catch (IOException ioEx)
                    {
                        Console.WriteLine($"[Warning] NetworkStream error (client may have disconnected): {ioEx.Message}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Error] Unexpected issue processing request: {ex.Message}");
                    }
                    
                    // Ensure connection is closed properly
                    client.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing client request: {ex.Message}");
                }
            }
        }
    }

    public static async Task Main()
    {
        TcpListener server = new TcpListener(IPAddress.Any, 9092);
        server.Start();
        Console.WriteLine("Kafka Server started on port 9092...");

        // Start processing tasks in the background
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
