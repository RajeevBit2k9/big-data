package big.data.practice.hbase

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.junit.{After, Before, Test}

/**
 * Set HADOOP_HOME in environment variable and HADOOP_HOME/bin in PATH
 * Set "test.build.data.basedirectory" to some writable folder "H:/workspace/test-data"
 *
 **/
class HBaseDaoTest {
  val utility = new HBaseTestingUtility()

  @Before
  @throws[Exception]
  def setUp(): Unit = {
    if (System.getenv().get("OS").startsWith("Windows")) {
      System.setProperty("test.build.data.basedirectory", "H:/workspace/test-data")
    } else {
      System.setProperty("hbase.local.dir", "/tmp")
    }

    utility.startMiniCluster()
  }

  @After
  @throws[Exception]
  def tearDown(): Unit = {
    utility.shutdownMiniCluster()
  }

  @Test
  def save(): Unit = {

    val dao = new HBaseDao
    val CF = "CF".getBytes()
    val fName = "fName".getBytes()
    val lName = "lName".getBytes()

    val table = utility.createTable(Bytes.toBytes("Student"), CF)
    val put = new Put(Bytes.toBytes("test1"))
    put.addColumn(CF, fName, Bytes.toBytes("Rajeev"))
    put.addColumn(CF, lName, Bytes.toBytes("Kumar"))

    dao.saveOneRecord(table, put)

    val get = new Get(Bytes.toBytes("test1"))
    val result = dao.getOneRecord(table, get)

    println(s"FirstName is ${Bytes.toString(result.getValue(CF, fName))}")
    println(s"LastName is ${Bytes.toString(result.getValue(CF, lName))}")
    assert(Bytes.toString(result.getValue(CF, fName)) == "Rajeev")
    assert(Bytes.toString(result.getValue(CF, lName)) == "Kumar")
  }
}
