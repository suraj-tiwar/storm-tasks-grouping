import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple}

import java.util

class FieldBolt extends BaseRichBolt{
  var collector : OutputCollector = _

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("Printer"))
  }

  override def prepare(topoConf: util.Map[String, AnyRef], context: TopologyContext, collector: OutputCollector): Unit = {
    this.collector = collector
  }

  override def execute(input: Tuple): Unit = {
    val fields = input.getFields
    val key = input.getValueByField("key")
    val value = input.getValueByField("value")
    println(fields)
    println(key+" "+value)
  }
}
