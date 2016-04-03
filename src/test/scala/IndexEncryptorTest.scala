package com.sjcampbell.spark.sse

import org.scalatest.FunSuite
import org.apache.hadoop.io._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

class IndexEncryptorTest extends FunSuite {
    
    test("IndexEncryptor generates the same hash when run twice") {
        val word1 = "word"
        val key1 = IndexEncryptor.GenerateWordKey(word1)
        val bytes1 = IndexEncryptor.EncryptWord(word1, key1, 0)
        
        val word2 = "word"
        val key2 = IndexEncryptor.GenerateWordKey(word2)
        val bytes2 = IndexEncryptor.EncryptWord(word2, key2, 0)
        
        println("Bytes length: " + bytes1.length)
        
        assert(bytes1.length > 0)
        
        for (i <- 0 to (bytes1.length - 1)) {
            assert(bytes1(i) == bytes2(i))
        }
    }
    
    test("Create different labels for different words") {
        val word1 = "this"
        val key1 = IndexEncryptor.GenerateWordKey(word1)
        val bytes1 = IndexEncryptor.EncryptWord(word1, key1, 0)
        
        val word2 = "this"
        val key2 = IndexEncryptor.GenerateWordKey(word2)
        val bytes2 = IndexEncryptor.EncryptWord(word2, key2, 1)
        
        assert(bytes1.length > 0)
        
        val bytesMatch = true
        assert(!bytes1.sameElements(bytes2), "The byte arrays returned by the label generator were the same, but should be different.")
    }
    
    test("IndexEncryptor generates the same hash when run thrice") {
        val word1 = "prognostication"
        val key1 = IndexEncryptor.GenerateWordKey(word1)
        val bytes1 = IndexEncryptor.EncryptWord(word1, key1, 0)
        
        val word2 = "prognostication"
        val key2 = IndexEncryptor.GenerateWordKey(word2)
        val bytes12 = IndexEncryptor.EncryptWord(word2, key2, 0)
        
        for (i <- 0 to (key1.length - 1)) {
            assert(key1(i) == key2(i), "Comparison failed of key1 and key2")
        }
        
        for (i <- 0 to (bytes1.length - 1)) {
            assert(bytes1(i) == bytes12(i), "Comparison failed of bytes1 and bytes12")
        }
        
        val word3 = "prognostication"
        val bytes3 = IndexEncryptor.EncryptWord(word3, key1, 0)

        assert(bytes1.length > 0)
        
        for (i <- 0 to (bytes1.length - 1)) {
            assert(bytes1(i) == bytes3(i))
        }
    }
    
    test("Generate static hash output size given varying word sizes") {
        // 2 Byte word
        val smallWord = "sw"
        // 50 Byte word
        val largeWord = "123456789012345678901234567890123456789012345678901234567890"
        
        val smallKey = IndexEncryptor.GenerateWordKey(smallWord)
        val largeKey = IndexEncryptor.GenerateWordKey(largeWord)
        
        var smallBytes = IndexEncryptor.EncryptWord(smallWord, smallKey, 0)
        var largeBytes = IndexEncryptor.EncryptWord(largeWord, largeKey, 1)
        assert(smallBytes.length == largeBytes.length)
    }
}