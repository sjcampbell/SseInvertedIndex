package com.sjcampbell.spark.sse

import org.bouncycastle.jcajce.provider.digest.SHA3

object IndexEncryptor {
    
    def EncryptWord(word: String, wordKey: Array[Byte], docCount: Int) : Array[Byte] = {
        val plainText = word + docCount.toString()
        
        // TODO: Use key in this crypto hash somehow... Maybe XOR with input, or 
        
        val md = new SHA3.DigestSHA3(224)
        md.update(plainText.getBytes())
        md.digest()
    }
    
    def GenerateWordKey(term: String): Array[Byte] = {
        ("1" + term).getBytes
    }
}