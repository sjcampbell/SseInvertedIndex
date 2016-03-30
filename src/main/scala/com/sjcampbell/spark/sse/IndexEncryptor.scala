package com.sjcampbell.spark.sse

import org.bouncycastle.jcajce.provider.digest.SHA3

object IndexEncryptor {
    
    def CryptoHashWord(word: String) : Array[Byte] = {
        val md = new SHA3.DigestSHA3(224)
        md.update(word.getBytes())
        md.digest()
    }
    
    def KeyedCryptoHash(word: String, key : Array[Byte], count : Int) : Array[Byte] = {
        
        // TODO: Use key in this crypto hash somehow... Maybe XOR with input, or 
        
        val md = new SHA3.DigestSHA3(224)
        md.update(word.getBytes())
        md.digest()
    }
    
    def GenerateWordKey(term: String): Array[Byte] = {
        ("1" + term).getBytes
    }
}