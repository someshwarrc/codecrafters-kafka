using System.Net.Sockets;
using System.Threading.Tasks;
namespace App.Kafka;
public static class StreamExtensions
{
    public static async Task WriteBigEndian(this NetworkStream stream, int input)
    {
        byte[] bytes = BitConverter.GetBytes(input);
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(bytes);
        }
        await stream.WriteAsync(bytes);
    }
}