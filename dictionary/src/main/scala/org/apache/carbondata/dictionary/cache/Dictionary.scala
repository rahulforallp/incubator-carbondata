package org.apache.carbondata.dictionary.cache

/**
 * Created by rahul on 24/2/17.
 */
trait Dictionary {
  def getSurrogateKey(value: String) : Int

  def getSurrogateKey(value: Byte[]) : Int

}
