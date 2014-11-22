using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Nector.Core.Tests
{
    public class UtilityTest
    {
        [Fact]
        public void ShouldReturnSameByteArray() 
        {
            //Arrange
            byte[] input=new byte[3]{0,1,2};

            //Act
            byte[] result = input.ToByteArray();

            //Aseert
            Assert.Equal(input, result);
        }

        [Fact]
        public void ShouldReturnByteArrayListFromCollection() 
        {
            //Arrange
            List<byte[]> expectedList = new List<byte[]>();
            List<object> inputList = new List<object>();
            int firstItemInInputList = 2;
            long secondItemInInputList = 3;
            string thirdItemInInputList= "sampleValue";
            expectedList.Add(BitConverter.GetBytes(firstItemInInputList));
            expectedList.Add(BitConverter.GetBytes(secondItemInInputList));
            expectedList.Add(Encoding.UTF8.GetBytes(thirdItemInInputList));
            inputList.Add(firstItemInInputList);
            inputList.Add(secondItemInInputList);
            inputList.Add(thirdItemInInputList);

            //Act
            List<byte[]> result = inputList.ToByteArrayListFromCollection<object>();

            //Aseert
            Assert.Equal<List<byte[]>>(expectedList, result);
        }
    }
}
