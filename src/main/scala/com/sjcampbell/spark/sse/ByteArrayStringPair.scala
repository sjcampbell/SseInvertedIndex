package com.sjcampbell.spark.sse

case class ByteArrayStringPair(encryptedWord: Array[Byte], word: String) extends Ordered[ByteArrayStringPair] {
    
    import scala.math.Ordered.orderingToOrdered
    
    def compare(that: ByteArrayStringPair) : Int = {
        
        val cmpEncryptedWord = compare(this.encryptedWord, that.encryptedWord)
        
        if (cmpEncryptedWord == 0) {
            this.word.compareTo(that.word)            
        }
        
        cmpEncryptedWord
    }
    

    // Thanks to "@Rex Kerr" on Stack Overflow for this comparison method (http://stackoverflow.com/a/7110446/2565692)
    // Efficiency could be improved a little here if arrays always exist and are always the same length.
    def compare(a: Array[Byte], b: Array[Byte]): Int = {
		if (a eq null) {
			if (b eq null) 0
			else -1
		}
		else if (b eq null) 1
		else {
		    val L = math.min(a.length, b.length)
		    var i = 0
			while (i < L) {
				if (a(i) < b(i)) return -1
				else if (b(i) < a(i)) return 1
					i += 1
			}
			if (L < b.length) -1
			else if (L < a.length) 1
			else 0
		}
    }
}