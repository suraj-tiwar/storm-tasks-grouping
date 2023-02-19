import org.apache.logging.log4j.Logger
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple}

import java.util

class TwoInputBolt extends BaseRichBolt{
  var collector : OutputCollector = _
  var list : List[Tuple] = _
  override def prepare(topoConf: util.Map[String, AnyRef], context: TopologyContext, collector: OutputCollector): Unit = {
    this.collector = collector
    list = List()
  }

  override def execute(input: Tuple): Unit = {
    val sourceId:String = input.getSourceComponent

    sourceId match {
      case "Message" => list = list:+(input)
      case "Ticker" => {
        println(input.getValue(0)+" "+sourceId)
        list.foreach(x => {
          println(x.getValue(0).toString +" "+ x.getSourceComponent+" "+ input.getSourceComponent)
          collector.ack(x)
        })
        list = list.empty
        collector.ack(input)
      }
      case _ => println("went something wrong")
    }

  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("Joiner"))
  }
}
