using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nector.Core
{
    public class Utility
    {
        public static byte[] ToByteArray(object obj)
        {
            if (obj == null)
            {
                return new byte[0];
            }
            else if (obj is byte[])
            {
                return (byte[])obj;
            }

            else if (obj is int)
            {
                return BitConverter.GetBytes((int)obj);
            }
            else if (obj is long)
            {
                return BitConverter.GetBytes((long)obj);
            }
            else if (obj is String)
            {
                return Encoding.UTF8.GetBytes((String)obj);
            }
            else if (obj is Guid)
            {
                return ToBigEndian((Guid)obj).ToByteArray();
            }
            else if (obj is Boolean)
            {
                return BitConverter.GetBytes((Boolean)obj);
            }
            else if (obj is DateTime)
            {
                return BitConverter.GetBytes(((DateTime)obj).Ticks);
            }
            else
            {
                return ToByteArray(obj.ToString());
            }
        }

      
        public static object ToObjectFromByteArray(byte[] array, Type t)
        {

            string typeName = t.ToString();
            switch (typeName)
            {
                case "System.String":
                    return Encoding.UTF8.GetString(array);
                case "System.DateTime":
                    return DateTime.FromBinary(BitConverter.ToInt64(array, 0));
                case "System.Int32":
                    return BitConverter.ToInt32(array, 0);
                case "System.Int64":
                    return BitConverter.ToInt64(array, 0);
                case "System.Guid":
                    if (array.Length == 32)
                    {
                        byte[] firstHalf = new byte[16];
                        byte[] secondHalf = new byte[16];
                        Array.Copy(array, firstHalf, 16);
                        Array.Copy(array, 16, secondHalf, 0, 16);
                        Guid firstHalfGuid = new Guid(firstHalf);
                        Guid secondHalfGuid = new Guid(secondHalf);
                        return new Guid[2] { firstHalfGuid, secondHalfGuid };
                    }
                    else
                    {
                        return new Guid(array);
                    }
                case "System.Boolean":
                    return BitConverter.ToBoolean(array, 0);
                case "System.Byte":
                case "System.Byte[]":
                    return array;
                default:
                    break;
            }
            return null;
        }


        public static List<byte[]> ToByteArrayListFromCollection<T>(IEnumerable<T> list)
        {
            List<byte[]> result = new List<byte[]>();
            foreach (var item in list)
            {
                result.Add(ToByteArray(item));
            }
            return result;
        }

        public static List<T> ToListFromByteArrayList<T>(List<byte[]> list)
        {
            List<T> result = new List<T>();
            foreach (var item in list)
            {
                result.Add((T)ToObjectFromByteArray(item,typeof(T)));
            }
            return result;
        }

        /// <summary>
        /// Converts little-endian .NET guids to big-endian Java guids:
        /// </summary>
        public static Guid ToBigEndian(Guid netGuid)
        {
            byte[] java = new byte[16];
            byte[] net = netGuid.ToByteArray();
            for (int i = 8; i < 16; i++)
            {
                java[i] = net[i];
            }
            java[0] = net[3];
            java[1] = net[2];
            java[2] = net[1];
            java[3] = net[0];
            java[4] = net[5];
            java[5] = net[4];
            java[6] = net[6];
            java[7] = net[7];
            return new Guid(java);
        }


    }
}
