package com.sjcampbell.spark.sse

import org.scalatest.FunSuite
import org.apache.hadoop.io.Text

class BuildInvertedIndexTest extends FunSuite {
    
    test("IndexEncryptor generates the same hash when run twice") {
        val word1 = "word"
        val bytes1 = IndexEncryptor.EncryptWord(word1, null, 0)
        
        val word2 = "word"
        val bytes2 = IndexEncryptor.EncryptWord(word2, null, 0)
        
        println("Bytes length: " + bytes1.length)
        
        assert(bytes1.length > 0)
        
        for (i <- 0 to (bytes1.length - 1)) {
            assert(bytes1(i) == bytes2(i))
        }
    }
    
    test("Generate static hash output size given varying word sizes") {
        // 2 Byte word
        val smallWord = "sw"
        // 50 Byte word
        val largeWord = "123456789012345678901234567890123456789012345678901234567890"
        
        var smallBytes = IndexEncryptor.EncryptWord(smallWord, null, 0)
        var largeBytes = IndexEncryptor.EncryptWord(largeWord, null, 1)
        assert(smallBytes.length == largeBytes.length)
    }
    
    test("CompareByteArrays") {
        val b1 = Array[Byte](1, 1, 1)
        val b2 = Array[Byte](2, 2, 2)
        val b3 = Array[Byte](2, 2, 2)
        
        val t1 = new Text(b1)
        val t2 = new Text(b2)
        val t3 = new Text(b3)
        
        val result1 = t1.compareTo(t2.getBytes, 0, t2.getLength)
        assert(result1 == -1)
        
        val result2 = t2.compareTo(t3.getBytes, 0, t3.getLength())
        assert(result2 == 0)
    }

    /* This test shows that the token parser doesn't parse out words that are separated by a comma but no spaces
    test("Tokenize") {
        val testStr = "For,she's not"
        val testStr2 = "The other, that she's in earth, from whence God send her quickly!"
        val result = TestTokenizer.tokenize(testStr2)
        //assert(result.length == 3)
    }
    
    object TestTokenizer extends Tokenizer {
        def testTokenize(str: String) : List[String] = {
            tokenize(str)
        }
    }
    */
}