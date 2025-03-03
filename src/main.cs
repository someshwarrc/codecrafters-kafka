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

async Task HandleClientAsync(TcpClient client)
{
    NetworkStream stream = client.GetStream();
    await stream.ParseHeader();
    await stream.FlushAsync();
}


while(true){
    TcpClient client = await server.AcceptTcpClientAsync();
    _=HandleClientAsync(client);
}
