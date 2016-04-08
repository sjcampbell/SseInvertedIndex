package com.sjcampbell.spark.sse

import org.bouncycastle.jcajce.provider.digest.SHA3
import java.nio.charset.Charset
import java.security.MessageDigest
import java.security.InvalidKeyException
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

object IndexEncryptor {
    val charSet = Charset.forName("US-ASCII");
    
    def EncryptWord(word: String, wordKey: Array[Byte], docCount: Int) : Array[Byte] = {

        val sha256_HMAC = Mac.getInstance("HmacSHA256");
        val plainText = word + docCount.toString()
        
        try
        {
            val secretKey = new SecretKeySpec(wordKey, 0, wordKey.length, "HmacSHA256")

            sha256_HMAC.init(secretKey)
            sha256_HMAC.doFinal(plainText.getBytes())
        }
        catch{
            case ex: Throwable => {
                println("!! Exception occurred while trying to encrypt word.")
                println("!! plaintext: " + plainText)
                throw ex
            }
        }

        /* SHA-256, non-HMAC
        val md2 = MessageDigest.getInstance("SHA-256")
        val hash = md2.digest(plainText.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        hash
        */
        
        /* SHA-3, non-HMAC
        val md = new SHA3.DigestSHA3(224)
        md.update(plainText.getBytes())
        md.digest()
        */
    }
    
    def GenerateWordKey(term: String): Array[Byte] = {
        ("1" + term).getBytes
    }
}