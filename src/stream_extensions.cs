using System.Net.Sockets;
using System.Threading.Tasks;
using System.Buffers.Binary;

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

    public static async Task ParseHeader(this NetworkStream stream) {
        byte[] binaryData = new byte[1024];
        await stream.ReadAsync(binaryData, 0, binaryData.Length);
        bool isLittleEndian = BitConverter.IsLittleEndian;    
        
        int correlation_id = BinaryPrimitives.ReadInt32BigEndian(binaryData[8..12]);

        Console.WriteLine($"Correlation Id: {correlation_id}");
        await stream.WriteBigEndian(correlation_id);
    }
}