public class FlinkTest (
public static void main(String(] args) throws Exception t
StreamExecutionEnvironment env = StreamExecutionEnvironnent.getExecutionEnvironment();
env. setRuntimeMode(RuntimeExecutionMode.BATCH);
DataStrean«String» source = env. fronELenents ("hello dfasd", "hello df", "dfasd", "hello");
DataStream«String> source = env.PeadTextfile( MePath "test.csv*)://.ma(x -> { Thread. sleep (100); /
DataStream<Tuple2«String, Long» count = source.flatHap(new FlatMapFunction«String, Tuple2«String,
@Override
public void flatMap(String s, Collector«Tuple2<String, Long» collector) throws Exception (
for (String so: s.split(reges" )) {
collector.collect(Tuple2.of (so, 1L));
7). keyBy(t -> t.fo
). window(TumblingProcessingTimeWindows.0f(Time.seconds(1))
). reduce(new Long>( Long>0
@Override
public Tuple2<String, Long> reduce(Tuple2<String, Long> a, Tuple2<String, Long> b) throws Exceptic
return Tuple2.of(a.f0, a.f1 + b.f1);

DataStream«String> top3 = count.windowAlL(TumbLingProcessingTimellindows.of(Time.seconds(1))
) -process (new ProcessAlWindowFunction<Tuple2<String, Long>, String, TimeWindow>0 1
Override
public void process(ProcessALlWindowFunction<Tuple2<String, Long>, String, TimeWindow>.Context conte
Iterable«Tuple2<String, Long> iterable,
Collector«String> out) throws Exception (
List<Tuple2<String, Long» List = new ArrayListe; iterable.forEach(list: :add);
list. sort((a, b) -> Long. compare(b.f1, a. f1)):
int n = 3;
StringBuilder sb = new StringBuilder (*Top* + n+- € • + context. windom C) getEnd + *: *):
for (int 1 = 0; i < Math.min(n, list.size()); i++) t
sb. append (List.get(i).f0) .append(*=*) -append(List.get(E).1) .append(* *);
out.collect(sb. toString());
List<Tuple2<String,
Long>> res = count.executeAndCollect(100):
res. forEach(System,out: printin);
top3. print);
eny,executeo
11-12
I
lems
Terminal
Build
Dependencies
mance: Exclude IDE and project directories from antivirus scans: // C:\Users\yxkj_lianruda\AppData\LocalVetBrains\/de
