using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using App.Kafka;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
TcpListener server = new TcpListener(IPAddress.Any, 9092);
server.Start();
// server.AcceptSocket(); // wait for client

TcpClient client = await server.AcceptTcpClientAsync();
try {
    NetworkStream stream = client.GetStream();
    int message_size = 0;
    await stream.WriteBigEndian(message_size);
    await stream.ParseHeader();
    await stream.FlushAsync();
} 
catch (SocketException ex)
{
    Console.WriteLine($"SocketException: {ex.Message}");
}
catch (Exception ex)
{
    Console.WriteLine($"Exception: {ex.Message}");
}
finally
{
    client.Close();
    server.Stop();
}