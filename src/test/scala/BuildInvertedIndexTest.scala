package com.sjcampbell.spark.sse

import org.scalatest.FunSuite

class BuildInvertedIndexTest extends FunSuite {
    
    test("IndexEncryptor generates the same hash when run twice") {
        val word1 = "word"
        val bytes1 = IndexEncryptor.CryptoHashWord(word1)
        
        val word2 = "word"
        val bytes2 = IndexEncryptor.CryptoHashWord(word2)
        
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
        
        var smallBytes = IndexEncryptor.CryptoHashWord(smallWord)
        var largeBytes = IndexEncryptor.CryptoHashWord(largeWord)
        
        assert(smallBytes.length == largeBytes.length)
    }
}