package com.sjcampbell.spark.sse

import org.bouncycastle.jcajce.provider.digest.SHA3
import java.security.MessageDigest

object IndexEncryptor {
    
    def EncryptWord(word: String, wordKey: Array[Byte], docCount: Int) : Array[Byte] = {
        val plainText = word + docCount.toString()
        
        // TODO: Use key in this crypto hash somehow... Maybe XOR with input. Can MAC-type algorithm be used?
        
        val md2 = MessageDigest.getInstance("SHA-256")
        val hash = md2.digest(plainText.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        hash
        
        /* Using SHA-3
        val md = new SHA3.DigestSHA3(224)
        md.update(plainText.getBytes())
        md.digest()
        */
    }
    
    def GenerateWordKey(term: String): Array[Byte] = {
        ("1" + term).getBytes
    }
}