package big.data.practice.hbase

import org.apache.hadoop.hbase.client.{Get, HTable, Put, Result}

class HBaseDao {

  def saveOneRecord(table: HTable, put: Put): Unit = {
    table.put(put)
  }

  def saveBatch(table: HTable, puts: java.util.List[Put]): Unit = {
    table.put(puts)
  }

  def getOneRecord(table: HTable, get: Get): Result = {
    table.get(get)
  }

  def batchGet(table: HTable, gets: java.util.List[Get]): Array[Result] = {
    table.get(gets)
  }
}
