using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using App.Kafka;

class KafkaServer
{
    public static async Task HandleClientAsync(TcpClient client)
    {
        Console.WriteLine("Client connected...");
        
        try
        {
            using NetworkStream stream = client.GetStream();
            
            while (client.Connected)
            {
                if (!stream.DataAvailable)
                {
                    await Task.Delay(10); // Small delay to prevent busy-waiting
                    continue;
                }

                try
                {
                    Console.WriteLine("Processing new request...");
                    
                    // Read and process API Version Request (v4)
                    await stream.ParseApiVersionRequest();

                    Console.WriteLine("Request processed successfully.");
                }
                catch (IOException ioEx)
                {
                    Console.WriteLine($"[Warning] Client disconnected unexpectedly: {ioEx.Message}");
                    break; // Exit loop on client disconnection
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Error] Failed to process request: {ex.Message}");
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Error] General error: {ex.Message}");
        }
        finally
        {
            client.Close();
            Console.WriteLine("Client connection closed.");
        }
    }

    public static async Task Main()
    {
        TcpListener server = new TcpListener(IPAddress.Any, 9092);
        server.Start();
        Console.WriteLine("Kafka Server started on port 9092...");

        try
        {
            while (true)
            {
                TcpClient client = await server.AcceptTcpClientAsync();
                _ = Task.Run(() => HandleClientAsync(client)); // Handle each client in a separate task
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"Server error: {e.Message}");
            server.Stop();
        }
    }
}
