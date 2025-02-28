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

    public static int CheckApiVersion(int api_version)
    {
        if (api_version >= 0 && api_version <= 4)
        {
            Console.WriteLine("UNSUPPORTED_VERSION");
            return 35; // Error Code for Unsupported Version
        }
        return 1;
    }
    public static int CheckApiVersion(int api_version)
    {
        if (api_version >= 0 && api_version <= 4)
        {
            Console.WriteLine("UNSUPPORTED_VERSION");
            return 35; // Error Code for Unsupported Version
        }
        return 1;
    }
    public static async Task ParseHeader(this NetworkStream stream) {
        byte[] binaryData = new byte[1024];
        
        int bytesRead = await stream.ReadAsync(binaryData, 0, binaryData.Length);
        
        int bytesRead = await stream.ReadAsync(binaryData, 0, binaryData.Length);
        bool isLittleEndian = BitConverter.IsLittleEndian;    
        
        int message_length = BinaryPrimitives.ReadInt16BigEndian(binaryData[0..4]);
        int api_key = BinaryPrimitives.ReadInt16BigEndian(binaryData[4..6]);
        int api_version = BinaryPrimitives.ReadInt16BigEndian(binaryData[6..8]);
        int correlation_id = BinaryPrimitives.ReadInt32BigEndian(binaryData[8..12]);

        Console.WriteLine($"Correlation Id: {correlation_id}");
        Console.WriteLine($"API Key: {api_key}");
        Console.WriteLine($"API Version: {api_version}");
        await stream.WriteBigEndian(correlation_id);
        await stream.WriteBigEndian(CheckApiVersion(api_version));
    }
}