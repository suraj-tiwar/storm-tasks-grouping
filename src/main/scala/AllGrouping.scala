import org.apache.storm.{Config, StormSubmitter}
import org.apache.storm.topology.TopologyBuilder

class AllGrouping {
  val builder = new TopologyBuilder()
  builder.setSpout("Message",new MsgSpout(),1)
  builder.setSpout("Ticker",new TickerSpout(),1)
  builder.setBolt("Merger",new TwoInputBolt(), 1).shuffleGrouping("Message").allGrouping("Ticker")
  try
    {
      StormSubmitter.submitTopology("TickerTopology",new Config(),builder.createTopology())
    }
  catch
    {
      case e => println(e)
    }
}

object AllGrouping {
  def main(args: Array[String]) : Unit ={
      new AllGrouping()
  }
}
