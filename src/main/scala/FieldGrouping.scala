import org.apache.storm.{Config, StormSubmitter}
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.tuple.Fields

class FieldGrouping {
  val builder = new TopologyBuilder()
  builder.setSpout("Spout",new FieldSpout())
  builder.setBolt("bolt",new FieldBolt(),4).customGrouping("Spout",new CustomLengthGrouping())
  try
    {
      StormSubmitter.submitTopology("FieldGrouping",new Config(),builder.createTopology())
    }
  catch
    {
      case e => e.printStackTrace()
    }
}
object FieldGrouping {
  def main(args : Array[String]): Unit = {
    new FieldGrouping()
  }
}
