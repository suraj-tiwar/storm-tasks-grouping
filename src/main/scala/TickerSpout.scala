import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Tuple, Values}

import java.util

class TickerSpout extends BaseRichSpout{
  var collector :SpoutOutputCollector = _

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("Ticker"))
  }

  override def open(conf: util.Map[String, AnyRef], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    this.collector = collector
  }

  override def nextTuple(): Unit = {
    Thread.sleep(20)
    collector.emit(new Values(true))
  }
}
